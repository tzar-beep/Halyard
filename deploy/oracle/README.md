# Deploying on Oracle Cloud Always Free

This runs the 3-node cluster on one Always Free Ampere VM, with HTTPS through
Caddy. Separately, `web/` is deployed to Vercel as the public demo page.

The free tier gives you a Linux VM that runs permanently. Vercel and Netlify can't
host the database itself, because it needs a long-running process, raw TCP and a
persistent disk.

## 1. Create the VM (about 10 minutes, in the Oracle console)

1. Create an instance with the image **Ubuntu 24.04** and shape **VM.Standard.A1.Flex**
   (2 OCPU / 12 GB is plenty). Add your SSH key.
2. Networking: in the VCN's security list, allow ingress TCP **80, 443**. If you want
   `redis-cli` access from outside, also allow **6381-6383**. In that case you must
   set a strong `REDIS_PASSWORD`, because open Redis ports are scanned within
   minutes.
3. Point a DNS name at the VM's public IP, for example `demo.yourdomain.com`.
   A free subdomain from a service such as DuckDNS also works.

## 2. Install and start

```bash
ssh ubuntu@<vm-ip>
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git
sudo usermod -aG docker ubuntu && newgrp docker
# Ubuntu images on OCI ship with iptables rules that block everything except SSH:
sudo iptables -I INPUT -p tcp -m multiport --dports 80,443,6381:6383 -j ACCEPT
sudo netfilter-persistent save
git clone <your-repo-url> redis && cd redis/deploy/oracle
cat > .env <<ENV
REDIS_PASSWORD=$(openssl rand -hex 24)
PUBLIC_HOST=demo.yourdomain.com
DEMO_ORIGIN=https://your-site.vercel.app
ENV
sed -i 's/demo.example.com/demo.yourdomain.com/' Caddyfile
docker compose up -d --build
curl https://demo.yourdomain.com/n1/api/status
```

## 3. Deploy the demo page to Vercel

In `web/config.js`, set:

```js
window.CLUSTER_NODES = [
  "https://demo.yourdomain.com/n1",
  "https://demo.yourdomain.com/n2",
  "https://demo.yourdomain.com/n3",
];
```

Then, from `web/`, run `npx vercel --prod`. Alternatively, import the repo in the
Vercel dashboard with **Root Directory = `web`** and no build command.

## Operating it

| Task | Command |
|---|---|
| Logs | `docker compose logs -f n1` |
| Raft state | `curl https://demo.yourdomain.com/n1/api/status` |
| Prometheus metrics | `curl` on port 8080 `/metrics` inside the network (not exposed publicly) |
| Rolling restart | `docker compose restart n1`, wait for `applied` to catch up, then the next node |
| Back up a node | `docker run --rm -v oracle_n1:/d -v $PWD:/b alpine tar czf /b/n1.tgz -C /d .` |

The demo API is rate-limited to 10 requests/s per IP. It refuses `FLUSHALL`, `KEYS`,
`AUTH`, transactions and cluster administration. Its "kill the leader" action freezes
a node for at most 15 s.
