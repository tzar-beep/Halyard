"use strict";

// redis-benchmark, -c 50 -P 16, persistence off (docs/benchmarks.md).
const BENCH = [
  ["SET", 613497, 735294],
  ["GET", 668896, 749064],
  ["INCR", 600601, 826446],
  ["LPUSH", 636943, 772201],
  ["ZADD", 617284, 706714],
];
const max = Math.max(...BENCH.flatMap((b) => [b[1], b[2]]));
const k = (n) => Math.round(n / 1000) + "k";
document.getElementById("bench-rows").innerHTML = BENCH.map(([name, h, r]) => `
  <div class="brow">
    <span class="bname">${name}</span>
    <div class="bars">
      <div class="bar-track"><div class="bar-h" style="width:${(h / max) * 100}%"></div></div>
      <div class="bar-track"><div class="bar-r" style="width:${(r / max) * 100}%"></div></div>
    </div>
    <span class="bnum">${k(h)}</span><span class="bnum dim">${k(r)}</span><span class="bnum ratio">${Math.round((h / r) * 100)}%</span>
  </div>`).join("");

// Copy buttons on commands.
document.querySelectorAll("code.cmd, pre.code").forEach((el) => {
  const b = document.createElement("button");
  b.className = "copy";
  b.type = "button";
  b.textContent = "Copy";
  b.addEventListener("click", async () => {
    const text = [...el.childNodes].filter((n) => !(n.classList && n.classList.contains("m")) && n !== b).map((n) => n.textContent).join("").trim();
    try { await navigator.clipboard.writeText(text); b.textContent = "Copied"; } catch { b.textContent = "Select & copy"; }
    setTimeout(() => { b.textContent = "Copy"; }, 1400);
  });
  el.appendChild(b);
});

document.querySelectorAll(".start-tabs button").forEach((b) => b.addEventListener("click", () => {
  document.querySelectorAll(".start-tabs button").forEach((x) => x.classList.toggle("active", x === b));
  document.querySelectorAll(".code[data-panel]").forEach((p) => { p.hidden = p.dataset.panel !== b.dataset.tab; });
}));

// Hero background: faint Raft traffic — nodes and packets flowing between them.
(() => {
  const c = document.getElementById("hero-bg");
  if (!c || matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  const ctx = c.getContext("2d");
  let W, H, nodes = [], pkts = [];
  const resize = () => {
    const r = devicePixelRatio || 1;
    W = c.clientWidth; H = c.clientHeight;
    c.width = W * r; c.height = H * r; ctx.setTransform(r, 0, 0, r, 0, 0);
    nodes = [...Array(Math.round(W / 70))].map(() => ({ x: Math.random() * W, y: Math.random() * H, l: Math.random() < 0.08 }));
  };
  resize(); addEventListener("resize", resize);
  const spawn = () => {
    const a = nodes[(Math.random() * nodes.length) | 0], b = nodes[(Math.random() * nodes.length) | 0];
    if (a !== b && Math.hypot(a.x - b.x, a.y - b.y) < 360) pkts.push({ a, b, t: 0, v: 0.004 + Math.random() * 0.006, g: a.l || Math.random() < 0.15 });
  };
  (function draw() {
    ctx.clearRect(0, 0, W, H);
    if (pkts.length < 45) spawn();
    for (const p of pkts) {
      p.t += p.v;
      const x = p.a.x + (p.b.x - p.a.x) * p.t, y = p.a.y + (p.b.y - p.a.y) * p.t;
      const tx = p.a.x + (p.b.x - p.a.x) * Math.max(0, p.t - 0.18), ty = p.a.y + (p.b.y - p.a.y) * Math.max(0, p.t - 0.18);
      const g = ctx.createLinearGradient(tx, ty, x, y);
      const col = p.g ? "63,185,80" : "236,236,238";
      g.addColorStop(0, `rgba(${col},0)`); g.addColorStop(1, `rgba(${col},${p.g ? 0.5 : 0.22})`);
      ctx.strokeStyle = g; ctx.lineWidth = 1.2;
      ctx.beginPath(); ctx.moveTo(tx, ty); ctx.lineTo(x, y); ctx.stroke();
    }
    pkts = pkts.filter((p) => p.t < 1);
    for (const n of nodes) {
      ctx.fillStyle = n.l ? "rgba(63,185,80,.55)" : "rgba(236,236,238,.13)";
      ctx.beginPath(); ctx.arc(n.x, n.y, n.l ? 2.4 : 1.6, 0, 7); ctx.fill();
    }
    requestAnimationFrame(draw);
  })();
})();
