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
