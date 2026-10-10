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
