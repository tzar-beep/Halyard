"use strict";

const NODES = window.CLUSTER_NODES;
const state = NODES.map((url, i) => ({ url, name: "n" + (i + 1), status: null, up: false }));
let lastLeader = null;
let lastTerm = 0;
let killAt = null; // when "kill the leader" was pressed
let killedName = null;

const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);

function logEvent(text) {
  const li = document.createElement("li");
  const t = new Date().toLocaleTimeString([], { hour12: false });
  li.innerHTML = `<time>${t}</time>${esc(text)}`;
  $("events").prepend(li);
}

async function poll(node) {
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 800);
    const r = await fetch(node.url + "/api/status", { signal: ctrl.signal });
    clearTimeout(timer);
    node.status = await r.json();
    node.up = true;
  } catch {
    node.up = false;
  }
}

function leaderNode() {
  // A paused node keeps reporting "leader" from its frozen state; trust the highest term.
  let best = null;
  for (const n of state) {
    const g = n.up && n.status && n.status.groups && n.status.groups[0];
    if (g && g.state === "leader" && (!best || g.term > best.status.groups[0].term)) best = n;
  }
  return best;
}

function render() {
  const leader = leaderNode();
  $("nodes").innerHTML = state.map((n) => {
    const g = n.up && n.status && n.status.groups ? n.status.groups[0] : null;
    const role = !n.up ? "down" : n === leader ? "leader" : g ? (g.state === "leader" ? "follower" : g.state) : "standalone";
    const extra = n.status && n.status.groups && n.status.groups.length > 1
      ? `<dt>groups led</dt><dd>${n.status.groups.filter((x) => x.state === "leader").map((x) => x.group).join(", ") || "none"}</dd>`
      : "";
    return `<article class="node ${role}">
      <h3>${esc(n.name)} <span class="badge ${role}">${role}</span></h3>
      <dl>
        <dt>term</dt><dd>${g ? g.term : "–"}</dd>
        <dt>commit / applied</dt><dd>${g ? g.commit + " / " + g.applied : "–"}</dd>
        <dt>keys</dt><dd>${n.up && n.status ? n.status.keys : "–"}</dd>
        <dt>p99 latency</dt><dd>${n.up && n.status ? n.status.p99_us + " µs" : "–"}</dd>
        ${extra}
      </dl>
    </article>`;
  }).join("");

  $("kill").disabled = !leader || killAt !== null;
  if (leader) {
    const term = leader.status.groups[0].term;
    if (leader.name !== lastLeader || term !== lastTerm) {
      if (lastLeader !== null) logEvent(`${leader.name} is leader (term ${term})`);
      if (killAt && leader.name !== killedName) {
        const ms = Math.round(performance.now() - killAt);
        $("failover").textContent = `New leader ${leader.name} elected ${ms} ms after ${killedName} went down`;
        logEvent(`failover completed in ${ms} ms (as observed by this page)`);
        killAt = null;
      }
      lastLeader = leader.name;
      lastTerm = term;
    }
  }
}

async function tick() {
  await Promise.all(state.map(poll));
  render();
}

$("kill").addEventListener("click", async () => {
  const leader = leaderNode();
  if (!leader) return;
  killedName = leader.name;
  killAt = performance.now();
  $("failover").textContent = `${leader.name} paused for 5 s…`;
  logEvent(`killed leader ${leader.name} (frozen for 5 s)`);
  try {
    await fetch(leader.url + "/api/fault", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "pause", seconds: 5 }),
    });
  } catch {
    logEvent("could not reach the leader");
    killAt = null;
  }
  setTimeout(() => {
    if (killAt) { $("failover").textContent = "Still waiting for a new leader…"; }
  }, 3000);
});

// --- console ---
const term = $("term");
const history = [];
let histPos = 0;

function print(text, cls) {
  const div = document.createElement("div");
  if (cls) div.className = cls;
  div.textContent = text;
  term.appendChild(div);
  term.scrollTop = term.scrollHeight;
}

function parseArgs(line) {
  const out = [];
  const re = /"((?:[^"\\]|\\.)*)"|'([^']*)'|(\S+)/g;
  let m;
  while ((m = re.exec(line))) out.push(m[1] !== undefined ? m[1].replace(/\\(.)/g, "$1") : m[2] !== undefined ? m[2] : m[3]);
  return out;
}

async function run(line) {
  const args = parseArgs(line);
  if (!args.length) return;
  print("redis> " + line, "cmd");
  let target = leaderNode() || state.find((n) => n.up);
  if (!target) { print("(error) no node reachable", "err"); return; }
  for (let hop = 0; hop < 3; hop++) {
    try {
      const r = await fetch(target.url + "/api/command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ args }),
      });
      if (r.status === 429) { print("(error) rate limited, slow down", "err"); return; }
      const { reply } = await r.json();
      const moved = /^\(error\) MOVED \d+ (\S+)/.exec(reply);
      if (moved) {
        const next = state.find((n) => n.status && n.status.addr === moved[1]);
        if (next && next !== target) { target = next; continue; }
      }
      print(reply, reply.startsWith("(error)") ? "err" : "");
      return;
    } catch {
      print("(error) request failed", "err");
      return;
    }
  }
}

$("form").addEventListener("submit", (e) => {
  e.preventDefault();
  const line = $("input").value.trim();
  $("input").value = "";
  if (!line) return;
  history.push(line);
  histPos = history.length;
  run(line);
});

$("input").addEventListener("keydown", (e) => {
  if (e.key === "ArrowUp" && histPos > 0) { $("input").value = history[--histPos]; e.preventDefault(); }
  if (e.key === "ArrowDown") { histPos = Math.min(history.length, histPos + 1); $("input").value = history[histPos] || ""; }
});

print("Connected to a 3-node Raft cluster. Type a command.");
tick();
setInterval(tick, 700);
