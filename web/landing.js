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
const k = (n) => Math.round(n / 1000) + "k req/s";
document.getElementById("bench-rows").innerHTML = BENCH.map(([name, h, r]) => `
  <div class="brow">
    <span class="bname">${name}</span>
    <div class="bars">
      <div class="bar-line"><div class="bar-track"><div class="bar-h" style="width:${(h / max) * 100}%"></div></div><span class="bval">${k(h)} · ${Math.round((h / r) * 100)}%</span></div>
      <div class="bar-line"><div class="bar-track"><div class="bar-r" style="width:${(r / max) * 100}%"></div></div><span class="bval">${k(r)}</span></div>
    </div>
  </div>`).join("");

// Quick-start tabs.
document.querySelectorAll(".start-tabs button").forEach((b) => b.addEventListener("click", () => {
  document.querySelectorAll(".start-tabs button").forEach((x) => x.classList.toggle("active", x === b));
  document.querySelectorAll(".code[data-panel]").forEach((p) => { p.hidden = p.dataset.panel !== b.dataset.tab; });
}));

// Live cluster status pill.
(async function live() {
  const el = document.getElementById("live");
  const txt = document.getElementById("live-text");
  const results = await Promise.all((window.CLUSTER_NODES || []).map(async (url) => {
    try {
      const ctrl = new AbortController();
      setTimeout(() => ctrl.abort(), 1500);
      const s = await (await fetch(url + "/api/status", { signal: ctrl.signal })).json();
      return s;
    } catch { return null; }
  }));
  const up = results.filter(Boolean);
  const leader = up.find((s) => s.groups && s.groups[0] && s.groups[0].state === "leader");
  if (leader) {
    el.className = "live-pill ok";
    txt.textContent = `Live cluster healthy · ${up.length}/${results.length} nodes · term ${leader.groups[0].term} — open dashboard →`;
  } else {
    el.className = "live-pill err";
    txt.textContent = "Live cluster offline — see how to run it locally →";
  }
})();

/* ---------- Cobe globe ---------- */
(async function globe() {
  const canvas = document.getElementById("globe");
  if (!canvas || false) return;
  let createGlobe;
  try { ({ default: createGlobe } = await import("https://cdn.jsdelivr.net/npm/cobe@0.6.3/+esm")); }
  catch { canvas.parentElement.hidden = true; return; }
  let phi = 0, width = 0, drag = null, dragOffset = 0;
  const resize = () => { width = canvas.offsetWidth; };
  addEventListener("resize", resize);
  resize();
  const g = createGlobe(canvas, {
    devicePixelRatio: 2, width: width * 2, height: width * 2,
    phi: 0, theta: 0.3, dark: 1, diffuse: 2, mapSamples: 22000, mapBrightness: 9,
    baseColor: [0.5, 0.5, 0.53], markerColor: [0.25, 0.73, 0.31], glowColor: [0.22, 0.22, 0.24],
    markers: [ // illustrative replica placement
      { location: [19.07, 72.88], size: 0.09 },
      { location: [50.11, 8.68], size: 0.06 },
      { location: [39.04, -77.49], size: 0.06 },
      { location: [1.35, 103.82], size: 0.05 },
      { location: [37.77, -122.42], size: 0.05 },
    ],
    onRender(state) {
      if (drag === null) phi += 0.004;
      state.phi = phi + dragOffset;
      state.width = width * 2;
      state.height = width * 2;
    },
  });
  canvas.addEventListener("pointerdown", (e) => { drag = e.clientX - dragOffset * 200; canvas.style.cursor = "grabbing"; });
  addEventListener("pointerup", () => { drag = null; canvas.style.cursor = "grab"; });
  addEventListener("pointermove", (e) => { if (drag !== null) dragOffset = (e.clientX - drag) / 200; });
  requestAnimationFrame(() => { canvas.style.opacity = "1"; });
  window.__globe = g;
})();

/* ---------- typing terminal ---------- */
(function typing() {
  const el = document.getElementById("typed");
  if (!el) return;
  const SCRIPT = [
    ["cmd", "docker compose -f deploy/docker-compose.cluster.yml up -d"],
    ["cmd", "redis-cli -c -p 6381 SET order:1042 paid"],
    ["out", "OK"],
    ["cmd", "docker stop deploy-n1-1", "# kill the leader"],
    ["cmd", "redis-cli -c -p 6382 GET order:1042"],
    ["note", "-> Redirected to slot [0] located at localhost:6383"],
    ["out", '"paid"', "# new leader elected, write intact"],
  ];
  const esc = (s) => s.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
  let html = "";
  const draw = (partial = "") => { el.innerHTML = html + partial + '<span class="cur"></span>'; };
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  async function play() {
    for (const [kind, text, comment] of SCRIPT) {
      const tail = comment ? `   <span class="m">${esc(comment)}</span>` : "";
      if (kind === "cmd") {
        for (let i = 1; i <= text.length; i++) { draw(`<span class="c">$</span> ${esc(text.slice(0, i))}`); await sleep(18 + Math.random() * 30); }
        html += `<span class="c">$</span> ${esc(text)}${tail}\n`;
        await sleep(350);
      } else {
        html += `<span class="${kind === "out" ? "o" : "m"}">${esc(text)}</span>${tail}\n`;
        await sleep(450);
      }
      draw();
    }
  }
  new IntersectionObserver((entries, obs) => { if (entries[0].isIntersecting) { obs.disconnect(); play(); } }).observe(el);
  draw();
})();

/* ---------- scroll-based velocity rows ---------- */
(function velocity() {
  const rows = [...document.querySelectorAll(".vrow")];
  if (!rows.length) return;
  rows.forEach((r) => { const t = r.querySelector(".vtrack"); t.textContent = t.textContent.repeat(6); });
  const pos = rows.map(() => 0);
  let lastY = scrollY, boost = 0, last = performance.now();
  function frame(now) {
    const dt = Math.min(64, now - last) / 1000; last = now;
    const v = (scrollY - lastY) / Math.max(dt, 0.001); lastY = scrollY;
    boost += (Math.min(6, Math.abs(v) / 300) * Math.sign(v || 1) - boost) * 0.1;
    rows.forEach((r, i) => {
      const t = r.firstElementChild, half = t.scrollWidth / 2, dir = +r.dataset.dir;
      pos[i] = (pos[i] - dir * (40 + 120 * Math.abs(boost)) * dt * (boost < 0 ? -1 : 1)) % half;
      if (pos[i] > 0) pos[i] -= half;
      t.style.transform = `translateX(${pos[i]}px)`;
    });
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
})();
