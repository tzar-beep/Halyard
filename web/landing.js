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
      <div class="bar-line"><div class="bar-track"><div class="bar-h" style="width:${(h / max) * 100}%"></div></div><span class="bval">${k(h)} <em>${Math.round((h / r) * 100)}%</em></span></div>
      <div class="bar-line"><div class="bar-track"><div class="bar-r" style="width:${(r / max) * 100}%"></div></div><span class="bval">${k(r)}</span></div>
    </div>
  </div>`).join("");

document.querySelectorAll(".start-tabs button").forEach((b) => b.addEventListener("click", () => {
  document.querySelectorAll(".start-tabs button").forEach((x) => x.classList.toggle("active", x === b));
  document.querySelectorAll(".code[data-panel]").forEach((p) => { p.hidden = p.dataset.panel !== b.dataset.tab; });
}));
