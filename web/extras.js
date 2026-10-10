"use strict";
// Raft log viewer, live charts, slot map, load generator, partition button and the
// guided tour. Uses globals from app.js: nodes, leader(), group(), event(), $, stopLeader.

/* ---------- partition & load ---------- */
$("iso-btn").addEventListener("click", () => stopLeader("isolate"));

$("load-btn").addEventListener("click", async () => {
  const l = leader();
  if (!l) return;
  const b = $("load-btn");
  b.disabled = true;
  b.textContent = "Writing…";
  event(`load: 1,000 writes sent to ${l.name}`);
  try {
    const r = await fetch(l.url + "/api/load", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ count: 1000 }),
    });
    const j = await r.json();
    event(`load: ${j.ok} committed in ${j.ms} ms (${j.opsPerSec.toLocaleString()} writes/s)${j.failed ? `, ${j.failed} failed` : ""}`, "ok");
  } catch {
    event("load: request failed", "err");
  }
  b.textContent = "Send 1,000 writes";
  b.disabled = false;
});

/* ---------- Raft log viewer ---------- */
const logState = {};
const seenMax = {};
async function pollLogs() {
  await Promise.all(nodes.map(async (n) => {
    if (!n.up) { logState[n.name] = null; return; }
    try {
      const ctrl = new AbortController();
      setTimeout(() => ctrl.abort(), 1000);
      logState[n.name] = await (await fetch(n.url + "/api/log", { signal: ctrl.signal, cache: "no-store" })).json();
    } catch { logState[n.name] = null; }
  }));
  renderLogs();
}

function renderLogs() {
  const l = leader();
  const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
  $("logcols").innerHTML = nodes.map((n) => {
    const lg = logState[n.name];
    const r = role(n, l);
    const prev = seenMax[n.name] || Infinity;
    if (lg && lg.entries && lg.entries.length) seenMax[n.name] = lg.entries[lg.entries.length - 1].index;
    const rows = !lg || r === "down"
      ? '<li class="lg-empty">unreachable</li>'
      : (lg.entries || []).slice().reverse().map((e) => `<li class="${e.index > lg.commit ? "pending" : ""} ${e.kind}${e.index > prev ? " fresh" : ""}">
          <span class="lg-idx">${e.index}</span><span class="lg-term t${e.term % 6}">T${e.term}</span><span class="lg-desc">${esc(e.desc)}</span></li>`).join("");
    return `<div class="logcol ${r}"><div class="logcol-head"><span>${n.name}</span><span class="role ${r}">${r}</span></div><ol>${rows}</ol></div>`;
  }).join("");
}
setInterval(pollLogs, 1200);
pollLogs();

/* ---------- charts ---------- */
const SERIES = 100;
const tp = [], lat = [], marks = [];
let lastLeaderName = null;

function chart(svg, values, fmt, unit) {
  const W = 600, H = 160, pad = 4;
  const max = Math.max(1, ...values) * 1.15;
  const x = (i) => (i / (SERIES - 1)) * W;
  const y = (v) => H - pad - (v / max) * (H - 2 * pad);
  const start = SERIES - values.length;
  const pts = values.map((v, i) => [x(start + i), y(v)]);
  let g = "";
  for (let k = 1; k <= 3; k++) g += `<line class="ch-grid" x1="0" x2="${W}" y1="${(H / 4) * k}" y2="${(H / 4) * k}"/>`;
  g += `<text class="ch-max" x="6" y="14">${fmt(max / 1.15)} ${unit}</text>`;
  for (const m of marks) {
    const age = SERIES - 1 - (tp.length - 1 - m.i);
    if (age >= 0 && age < SERIES) g += `<line class="ch-mark" x1="${x(age)}" x2="${x(age)}" y1="0" y2="${H}"/><text class="ch-mark-t" x="${x(age) + 4}" y="${H - 8}">${m.label}</text>`;
  }
  if (pts.length > 1) {
    const d = pts.map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
    g += `<path class="ch-area" d="${d} L${pts[pts.length - 1][0]} ${H} L${pts[0][0]} ${H}Z"/><path class="ch-line" d="${d}"/>`;
    const lp = pts[pts.length - 1];
    g += `<circle class="ch-dot" cx="${lp[0]}" cy="${lp[1]}" r="3.5"/>`;
  }
  svg.innerHTML = g;
}

setInterval(() => {
  const l = leader();
  const rate = l ? Math.round(l.hist[l.hist.length - 1] || 0) : 0;
  const p99 = l && l.status ? l.status.p99_us : 0;
  tp.push(rate); lat.push(p99);
  if (tp.length > SERIES) { tp.shift(); lat.shift(); marks.forEach((m) => m.i--); }
  if (l && l.name !== lastLeaderName) {
    if (lastLeaderName) marks.push({ i: tp.length - 1, label: `${l.name} leader` });
    lastLeaderName = l.name;
  }
  while (marks.length && marks[0].i < 0) marks.shift();
  chart($("chart-tp"), tp, (v) => Math.round(v).toLocaleString(), "/s");
  chart($("chart-lat"), lat, (v) => Math.round(v).toLocaleString(), "µs");
  $("tp-now").textContent = `${rate.toLocaleString()} entries / s`;
  $("lat-now").textContent = `${(p99 || 0).toLocaleString()} µs`;
}, 600);

/* ---------- slot map (sharded clusters) ---------- */
const GROUP_COLORS = ["#3fb950", "#6f8cff", "#d29922", "#c678dd", "#56b6c2", "#e06c75"];
async function pollSlots() {
  const n = nodes.find((x) => x.up);
  if (!n) return;
  try {
    const j = await (await fetch(n.url + "/api/slots", { cache: "no-store" })).json();
    if (!j.sharded) return;
    $("slots").hidden = false;
    const c = $("slotmap"), ctx = c.getContext("2d");
    const cols = 256, cell = c.width / cols, rowsH = c.height / (16384 / cols);
    ctx.clearRect(0, 0, c.width, c.height);
    const groups = new Map();
    for (const r of j.ranges) {
      ctx.fillStyle = GROUP_COLORS[(r.group - 1) % GROUP_COLORS.length];
      groups.set(r.group, (groups.get(r.group) || 0) + (r.end - r.start + 1));
      for (let s = r.start; s <= r.end; s++) ctx.fillRect((s % cols) * cell, Math.floor(s / cols) * rowsH, cell - 0.6, rowsH - 0.6);
    }
    $("slot-legend").innerHTML = [...groups].map(([g, cnt]) => `<span><i style="background:${GROUP_COLORS[(g - 1) % GROUP_COLORS.length]}"></i>group ${g} · ${cnt.toLocaleString()} slots</span>`).join("");
  } catch {}
}
setInterval(pollSlots, 3000);
pollSlots();

/* ---------- guided tour ---------- */
const TOUR = [
  { el: "overview", title: "A live three-node cluster", text: "Each card is a real server. The green one is the Raft leader: every write goes through it and is replicated to the others before it is acknowledged." },
  { el: "topo", title: "Replication, packet by packet", text: "Green comets are log entries, white ones heartbeats, blue ones acknowledgements. Let's generate some traffic.", act: () => $("load-btn").click() },
  { el: "raftlog", title: "The same log on every node", text: "Each column is one replica's log. Entries appear on the leader first, then on the followers; faded rows are not committed yet." },
  { el: "kill", title: "Now break it", text: "We'll stop the leader. Followers stop receiving heartbeats, time out, and elect a new leader by majority vote.", act: () => stopLeader("pause") },
  { el: "fo-strip", title: "Recovered", text: "A new leader took over within about a second and kept serving writes; nothing acknowledged was lost. Try “Partition leader” too: the cut-off leader steps down on its own." },
];
let tourAt = -1;
function showTour(i) {
  document.querySelectorAll(".tour-target").forEach((e) => e.classList.remove("tour-target"));
  if (i < 0 || i >= TOUR.length) { $("tour").hidden = true; tourAt = -1; return; }
  tourAt = i;
  const s = TOUR[i];
  const target = $(s.el) || document.querySelector("." + s.el);
  target.classList.add("tour-target");
  target.scrollIntoView({ behavior: "smooth", block: "center" });
  $("tour").hidden = false;
  $("tour-step").textContent = `Step ${i + 1} of ${TOUR.length}`;
  $("tour-title").textContent = s.title;
  $("tour-text").textContent = s.text;
  $("tour-back").disabled = i === 0;
  $("tour-next").textContent = i === TOUR.length - 1 ? "Finish" : s.act ? "Do it" : "Next";
}
$("tour-btn").addEventListener("click", () => showTour(0));
if (location.hash === "#tour") setTimeout(() => showTour(0), 1200);
$("tour-skip").addEventListener("click", () => showTour(-1));
$("tour-back").addEventListener("click", () => showTour(tourAt - 1));
$("tour-next").addEventListener("click", () => {
  const s = TOUR[tourAt];
  if (s && s.act) s.act();
  showTour(tourAt + 1);
});
document.addEventListener("keydown", (e) => { if (e.key === "Escape" && tourAt >= 0) showTour(-1); });
