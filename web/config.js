// HTTP API base URL of each cluster node (the server's -http address, served over
// HTTPS in production, e.g. through Caddy). The defaults match deploy/docker-compose.cluster.yml.
window.CLUSTER_NODES = window.CLUSTER_NODES || [
  "http://localhost:8081",
  "http://localhost:8082",
  "http://localhost:8083",
];
