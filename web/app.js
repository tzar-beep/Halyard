"use strict";

const NODES = window.CLUSTER_NODES;
const POLL_MS = 600;
const HISTORY = 30;
const PAUSE_S = 5;

const nodes = NODES.map((url, i) => ({ url, name: "n" + (i + 1), status: null, up: false, hist: [], lastCommit: null }));
const elections = [];
let lastLeader = null;
let lastTerm = 0;
let kill = null; // { at, victim, timer }

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
const num = (n) => (n == null ? "—" : Number(n).toLocaleString("en-US"));
const clock = () => new Date().toLocaleTimeString("en-GB", { hour12: false });

/* ---------- data ---------- */
async function poll(n) {
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 900);
    const r = await fetch(n.url + "/api/status", { signal: ctrl.signal, cache: "no-store" });
    clearTimeout(t);
    n.status = await r.json();
    n.up = true;
    const g = group(n);
    const c = g ? g.commit : 0;
    n.hist.push(n.lastCommit == null ? 0 : Math.max(0, (c - n.lastCommit) * (1000 / POLL_MS)));
    n.lastCommit = c;
  } catch {
    n.up = false;
    n.hist.push(0);
  }
  if (n.hist.length > HISTORY) n.hist.shift();
}

const group = (n) => (n.up && n.status && n.status.groups && n.status.groups[0]) || null;
const frozen = (n) => kill && kill.victim === n.name && performance.now() - kill.at < PAUSE_S * 1000;

// A frozen node keeps reporting its stale state; the highest-term leader wins.
function leader() {
  let best = null;
  for (const n of nodes) {
    const g = group(n);
    if (g && g.state === "leader" && !frozen(n) && (!best || g.term > group(best).term)) best = n;
  }
  return best;
}

function role(n, l) {
  if (!n.up || frozen(n)) return "down";
  const g = group(n);
  if (!g) return "follower";
  if (n === l) return "leader";
  return g.state === "leader" ? "follower" : g.state;
}

/* ---------- events ---------- */
function event(text, kind = "") {
  const ol = $("events");
  ol.querySelector(".placeholder")?.remove();
  const li = document.createElement("li");
  li.innerHTML = `<time>${clock()}</time><span class="${kind}">${esc(text)}</span>`;
  ol.prepend(li);
  while (ol.children.length > 80) ol.lastChild.remove();
}
function resetEvents() { $("events").innerHTML = '<li class="placeholder"><time></time><span class="empty">No events yet.</span></li>'; }
$("clear").addEventListener("click", resetEvents);
resetEvents();

/* ---------- topology ---------- */
const POS = [[240, 46], [110, 196], [370, 196]];
const BOX_W = 132, BOX_H = 52;

function renderTopo(l) {
  const roles = nodes.map((n) => role(n, l));
  let out = "";
  for (let i = 0; i < nodes.length; i++)
    for (let j = i + 1; j < nodes.length; j++)
      out += `<line class="t-edge" x1="${POS[i][0]}" y1="${POS[i][1]}" x2="${POS[j][0]}" y2="${POS[j][1]}"/>`;
  if (l) {
    const li = nodes.indexOf(l);
    nodes.forEach((n, i) => {
      if (i !== li && roles[i] !== "down")
        out += `<line class="t-flow" x1="${POS[li][0]}" y1="${POS[li][1]}" x2="${POS[i][0]}" y2="${POS[i][1]}"/>`;
    });
  }
  nodes.forEach((n, i) => {
    const [cx, cy] = POS[i];
    const x = cx - BOX_W / 2, y = cy - BOX_H / 2;
    const r = roles[i];
    const g = group(n);
    out += `<rect class="t-box ${r}" x="${x}" y="${y}" width="${BOX_W}" height="${BOX_H}" rx="6"/>
      <circle class="t-dot ${r}" cx="${x + 16}" cy="${y + 19}" r="3.5"/>
      <text class="t-name" x="${x + 28}" y="${y + 23}">${n.name}</text>
      <text class="t-role ${r}" x="${x + BOX_W - 12}" y="${y + 23}" text-anchor="end">${r}</text>
      <text class="t-meta" x="${x + 16}" y="${y + 41}">${g && r !== "down" ? `term ${g.term} · idx ${num(g.commit)}` : "no heartbeat"}</text>`;
  });
  $("topo").innerHTML = out;
}

/* ---------- nodes table ---------- */
function spark(values) {
  const w = 120, h = 24, max = Math.max(1, ...values);
  if (values.length < 2) return "";
  const pts = values.map((v, i) => [(i / (HISTORY - 1)) * w, h - 2 - (v / max) * (h - 4)]);
  const d = pts.map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
  const area = `${d} L${pts[pts.length - 1][0].toFixed(1)} ${h} L${pts[0][0].toFixed(1)} ${h}Z`;
  return `<svg class="spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" aria-hidden="true"><path class="area" d="${area}"/><path d="${d}"/></svg>`;
}

function renderNodes(l) {
  $("node-rows").innerHTML = nodes.map((n) => {
    const r = role(n, l);
    const g = r === "down" ? null : group(n);
    const s = n.status || {};
    return `<tr>
      <td class="name">${n.name}</td>
      <td class="addr">${esc(s.addr || n.url.replace(/^https?:\/\//, ""))}</td>
      <td><span class="role ${r}">${r}</span></td>
      <td class="num">${g ? g.term : "—"}</td>
      <td class="num">${g ? num(g.commit) : "—"}</td>
      <td class="num">${g ? num(g.applied) : "—"}</td>
      <td class="num">${g ? num(s.keys) : "—"}</td>
      <td class="num">${g ? num(s.p99_us) + " µs" : "—"}</td>
      <td>${spark(n.hist)}</td>
    </tr>`;
  }).join("");
}

/* ---------- header, stats, failover ---------- */
function renderStats(l) {
  const up = nodes.filter((n) => n.up && !frozen(n)).length;
  const g = l && group(l);
  const st = $("status");
  st.className = "status " + (l && up === nodes.length ? "ok" : l ? "warn" : "err");
  $("status-text").textContent = l
    ? `${up === nodes.length ? "Healthy" : "Degraded"} · ${up}/${nodes.length} nodes · term ${g.term}`
    : `Electing · ${up}/${nodes.length} nodes`;
  $("s-leader").textContent = l ? l.name : "electing";
  $("s-leader").className = "value" + (l ? " ok" : "");
  $("s-term").textContent = g ? g.term : "—";
  $("s-commit").textContent = g ? num(g.commit) : "—";
  $("s-rate").textContent = l ? Math.round(l.hist[l.hist.length - 1] || 0) : "—";
  $("term-title").textContent = l ? `redis-cli · ${l.name} (leader)` : "redis-cli · no leader";
}

function renderElections() {
  $("elections").innerHTML = elections.length
    ? elections.slice(-6).reverse().map((e) => `<tr><td>${e.term}</td><td>${e.leader}</td><td>${e.at}</td></tr>`).join("")
    : '<tr><td colspan="3" class="empty">No elections observed yet.</td></tr>';
}

function track(l) {
  $("kill").disabled = !l || !!kill;
  if (!l) return;
  const term = group(l).term;
  if (l.name === lastLeader && term === lastTerm) return;
  elections.push({ term, leader: l.name, at: clock() });
  renderElections();
  if (lastLeader !== null) event(`${l.name} elected leader, term ${term}`, "ok");
  if (kill && l.name !== kill.victim) {
    const ms = Math.round(performance.now() - kill.at);
    clearInterval(kill.timer);
    $("fo").className = "failover done";
    $("fo-state").textContent = `Recovered — ${l.name} is leader`;
    $("fo-time").textContent = `${(ms / 1000).toFixed(2)} s`;
    $("fo-bar").style.width = "100%";
    $("fo-sub").textContent = `${kill.victim} was unreachable; the remaining majority elected ${l.name} in term ${term}.`;
    $("s-failover").textContent = `${ms} ms`;
    event(`failover completed in ${ms} ms`);
    const victim = kill.victim;
    setTimeout(() => { if (kill && kill.victim === victim) kill = null; }, Math.max(0, PAUSE_S * 1000 - ms + 300));
  }
  lastLeader = l.name;
  lastTerm = term;
}

async function stopLeader() {
  const l = leader();
  if (!l || kill) return;
  kill = { at: performance.now(), victim: l.name };
  $("kill").disabled = true;
  $("fo").className = "failover active";
  $("fo-state").textContent = `${l.name} stopped — election pending`;
  $("fo-sub").textContent = "Followers wait for the randomized election timeout, then vote.";
  event(`${l.name} stopped by operator (${PAUSE_S} s)`, "err");
  kill.timer = setInterval(() => {
    const s = (performance.now() - kill.at) / 1000;
    $("fo-time").textContent = `${s.toFixed(1)} s`;
    $("fo-bar").style.width = `${Math.min(100, (s / 3) * 100)}%`;
  }, 50);
  try {
    await fetch(l.url + "/api/fault", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "pause", seconds: PAUSE_S }),
    });
  } catch {
    clearInterval(kill.timer);
    kill = null;
    event("could not reach the leader", "err");
  }
}
$("kill").addEventListener("click", stopLeader);

/* ---------- loop ---------- */
function render() {
  const l = leader();
  track(l);
  renderStats(l);
  renderTopo(l);
  renderNodes(l);
}
async function tick() {
  await Promise.all(nodes.map(poll));
  render();
  $("clock").textContent = clock();
}

/* ---------- console ---------- */
const out = $("term");
const hist = [];
let hpos = 0;
const PRESETS = ["SET greeting hello", "GET greeting", "INCR visits", "ZADD board 42 alice 17 bob",
  "ZRANGE board 0 -1 WITHSCORES", "HSET user:1 name ada", "HGETALL user:1", "INFO replication"];

$("presets").innerHTML = PRESETS.map((p) => `<button type="button">${esc(p)}</button>`).join("");
$("presets").addEventListener("click", (e) => { if (e.target.tagName === "BUTTON") run(e.target.textContent); });

function print(html, cls) {
  const d = document.createElement("div");
  if (cls) d.className = cls;
  d.innerHTML = html;
  out.appendChild(d);
  out.scrollTop = out.scrollHeight;
}

function parse(line) {
  const res = [];
  const re = /"((?:[^"\\]|\\.)*)"|'([^']*)'|(\S+)/g;
  let m;
  while ((m = re.exec(line))) res.push(m[1] !== undefined ? m[1].replace(/\\(.)/g, "$1") : m[2] !== undefined ? m[2] : m[3]);
  return res;
}

async function run(line) {
  const args = parse(line);
  if (!args.length) return;
  hist.push(line);
  hpos = hist.length;
  let target = leader() || nodes.find((n) => n.up);
  if (!target) { print("(error) no node reachable", "err"); return; }
  const t0 = performance.now();
  for (let hop = 0; hop < 3; hop++) {
    try {
      const r = await fetch(target.url + "/api/command", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ args }),
      });
      if (r.status === 429) { print("(error) rate limited", "err"); return; }
      const { reply } = await r.json();
      const moved = /^\(error\) MOVED \d+ (\S+)/.exec(reply);
      if (moved) {
        const next = nodes.find((n) => n.status && n.status.addr === moved[1]);
        if (next && next !== target) { target = next; continue; }
      }
      print(`&gt; ${esc(line)}  <span class="meta">${target.name} · ${Math.round(performance.now() - t0)} ms</span>`, "cmd");
      print(esc(reply), reply.startsWith("(error)") ? "err" : "");
      return;
    } catch {
      print("(error) request failed", "err");
      return;
    }
  }
}

$("form").addEventListener("submit", (e) => {
  e.preventDefault();
  const v = $("input").value.trim();
  $("input").value = "";
  if (v) run(v);
});
$("input").addEventListener("keydown", (e) => {
  if (e.key === "ArrowUp" && hpos > 0) { $("input").value = hist[--hpos]; e.preventDefault(); }
  if (e.key === "ArrowDown") { hpos = Math.min(hist.length, hpos + 1); $("input").value = hist[hpos] || ""; }
});

/* active tab follows scroll */
const sections = ["overview", "nodes", "console", "design"].map((id) => $(id));
addEventListener("scroll", () => {
  let cur = sections[0].id;
  for (const s of sections) if (s.getBoundingClientRect().top < 120) cur = s.id;
  document.querySelectorAll(".tabs a").forEach((a) => a.classList.toggle("active", a.getAttribute("href") === "#" + cur));
}, { passive: true });

print("Connected. Choose a preset or type a command.", "sys");
renderElections();
tick();
setInterval(tick, POLL_MS);
