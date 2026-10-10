"use strict";
// Network pipeline view: leader in the centre, followers left and right, each link a
// three-lane pipe (appends →, heartbeats →, acks ←) carrying comet-shaped packets.
window.Topo = (() => {
  const NS = "http://www.w3.org/2000/svg";
  const W = 1160, H = 250, CY = 118;
  const SLOT_X = [170, 580, 990];          // left, centre, right
  const CW = 224, CH = 138;                // card size
  const LANE = { append: -16, beat: 0, ack: 16 };
  const BLOCKS = 16, TAIL = 7;
  let svg, pipeLayer, pktLayer, cards = [], pipes = {}, packets = [], pool = [];
  let st = { roles: [], leader: -1, rate: 0, slots: [0, 1, 2] };
  let lastBeat = 0, acc = 0, last = performance.now();

  const el = (tag, a = {}, p) => { const e = document.createElementNS(NS, tag); for (const k in a) e.setAttribute(k, a[k]); if (p) p.appendChild(e); return e; };

  function init(svgEl, names) {
    svg = svgEl;
    svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
    const defs = el("defs", {}, svg);
    const pat = el("pattern", { id: "grid", width: 24, height: 24, patternUnits: "userSpaceOnUse" }, defs);
    el("path", { d: "M24 0H0V24", class: "gridline" }, pat);
    const vg = el("radialGradient", { id: "spot", cx: "50%", cy: "50%", r: "50%" }, defs);
    el("stop", { offset: "0", "stop-color": "#3fb950", "stop-opacity": "0.10" }, vg);
    el("stop", { offset: "1", "stop-color": "#3fb950", "stop-opacity": "0" }, vg);
    const sh = el("filter", { id: "lift", x: "-20%", y: "-20%", width: "140%", height: "160%" }, defs);
    el("feDropShadow", { dx: 0, dy: 8, stdDeviation: 10, "flood-color": "#000", "flood-opacity": 0.55 }, sh);
    const gl = el("filter", { id: "pglow", x: "-300%", y: "-300%", width: "700%", height: "700%" }, defs);
    el("feGaussianBlur", { stdDeviation: 3, result: "b" }, gl);
    const m = el("feMerge", {}, gl); el("feMergeNode", { in: "b" }, m); el("feMergeNode", { in: "SourceGraphic" }, m);

    el("rect", { width: W, height: H, fill: "url(#grid)" }, svg);
    el("ellipse", { cx: SLOT_X[1], cy: CY, rx: 360, ry: 170, fill: "url(#spot)", class: "spot" }, svg);

    // Pipes: one per side (left/right of centre).
    pipeLayer = el("g", {}, svg);
    for (const side of ["L", "R"]) {
      const g = el("g", { class: "pipe" }, pipeLayer);
      const x1 = side === "L" ? SLOT_X[0] + CW / 2 : SLOT_X[1] + CW / 2;
      const x2 = side === "L" ? SLOT_X[1] - CW / 2 : SLOT_X[2] - CW / 2;
      el("rect", { x: x1, y: CY - 30, width: x2 - x1, height: 60, rx: 10, class: "pipe-body" }, g);
      for (const k in LANE) el("line", { x1, x2, y1: CY + LANE[k], y2: CY + LANE[k], class: "lane " + k }, g);
      const lbl = el("text", { x: (x1 + x2) / 2, y: CY - 40, class: "pipe-label", "text-anchor": "middle" }, g);
      pipes[side] = { g, x1, x2, lbl };
    }
    // Follower ↔ follower: dim arc underneath.
    el("path", { d: `M${SLOT_X[0]} ${CY + CH / 2} Q${SLOT_X[1]} ${H + 10} ${SLOT_X[2]} ${CY + CH / 2}`, class: "peer-arc" }, svg);
    pktLayer = el("g", {}, svg);

    cards = names.map((name, i) => {
      const g = el("g", { class: "nc follower" }, svg);
      g.style.transform = `translate(${SLOT_X[i]}px, ${CY}px)`;
      el("rect", { x: -CW / 2 - 7, y: -CH / 2 - 7, width: CW + 14, height: CH + 14, rx: 18, class: "ring" }, g);
      el("rect", { x: -CW / 2, y: -CH / 2, width: CW, height: CH, rx: 14, class: "body", filter: "url(#lift)" }, g);
      el("rect", { x: -CW / 2, y: -CH / 2, width: CW, height: 3, rx: 1.5, class: "accent" }, g);
      el("circle", { cx: -CW / 2 + 22, cy: -CH / 2 + 28, r: 4.5, class: "led" }, g);
      const t = el("text", { x: -CW / 2 + 36, y: -CH / 2 + 34, class: "nc-name" }, g); t.textContent = name;
      el("rect", { x: CW / 2 - 92, y: -CH / 2 + 15, width: 76, height: 24, rx: 12, class: "pill" }, g);
      const role = el("text", { x: CW / 2 - 54, y: -CH / 2 + 31, class: "nc-role", "text-anchor": "middle" }, g);
      const kv = (x, label) => {
        const l = el("text", { x, y: -2, class: "nc-k" }, g); l.textContent = label;
        return el("text", { x, y: 18, class: "nc-v" }, g);
      };
      const term = kv(-CW / 2 + 22, "TERM"), idx = kv(-CW / 2 + 92, "INDEX"), lag = kv(-CW / 2 + 170, "LAG");
      const bw = (CW - 44 - (BLOCKS - 1) * 3) / BLOCKS;
      const blocks = [...Array(BLOCKS)].map((_, k) => el("rect", { x: -CW / 2 + 22 + k * (bw + 3), y: CH / 2 - 30, width: bw, height: 12, rx: 2, class: "blk" }, g));
      return { g, role, term, idx, lag, blocks };
    });
    requestAnimationFrame(frame);
  }

  const slotOf = (node) => st.slots.indexOf(node); // 0 left, 1 centre, 2 right

  function spawn(fromSlot, toSlot, kind) {
    const side = Math.min(fromSlot, toSlot) === 0 ? "L" : "R";
    const p = pipes[side];
    const forward = toSlot > fromSlot;
    const dots = pool.pop() || [...Array(TAIL)].map(() => el("circle", {}, pktLayer));
    dots.forEach((d, i) => {
      d.setAttribute("class", "pkt " + kind);
      d.setAttribute("r", (kind === "append" ? 4.2 : kind === "beat" ? 3.2 : 2.8) * (1 - i / (TAIL + 1)));
      d.setAttribute("opacity", (1 - i / TAIL).toFixed(2));
      d.setAttribute("filter", i === 0 ? "url(#pglow)" : "");
      d.style.display = "";
    });
    packets.push({ x1: forward ? p.x1 : p.x2, x2: forward ? p.x2 : p.x1, y: CY + LANE[kind], t: 0, kind, dots, from: fromSlot, to: toSlot });
  }

  function frame(now) {
    const dt = Math.min(0.05, (now - last) / 1000);
    last = now;
    const { roles, leader, rate } = st;
    if (leader >= 0) {
      const live = [0, 2].filter((s) => roles[st.slots[s]] !== "down");
      if (now - lastBeat > 700) { lastBeat = now; live.forEach((s) => spawn(1, s, "beat")); }
      acc += Math.min(rate, 12) * dt;
      while (acc >= 1) { acc -= 1; live.forEach((s) => spawn(1, s, "append")); }
    }
    packets = packets.filter((p) => {
      p.t += (p.kind === "ack" ? 1.25 : 0.95) * dt;
      if (p.t >= 1 + TAIL * 0.025) {
        p.dots.forEach((d) => { d.style.display = "none"; });
        pool.push(p.dots);
        if (p.kind !== "ack" && roles[st.slots[p.to]] !== "down") spawn(p.to, p.from, "ack");
        return false;
      }
      p.dots.forEach((d, i) => {
        const t = Math.max(0, Math.min(1, p.t - i * 0.025));
        const e = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2; // ease in-out
        d.setAttribute("cx", (p.x1 + (p.x2 - p.x1) * e).toFixed(1));
        d.setAttribute("cy", p.y);
        d.style.visibility = p.t - i * 0.025 < 0 || p.t - i * 0.025 > 1 ? "hidden" : "visible";
      });
      return true;
    });
    requestAnimationFrame(frame);
  }

  // info[i] = { role, term, commit, applied }
  function update(info, leaderIdx, rate) {
    // Leader takes the centre slot; the others keep their relative order.
    const slots = leaderIdx >= 0 ? [...info.keys()].filter((i) => i !== leaderIdx) : [0, 2];
    st = { roles: info.map((x) => x.role), leader: leaderIdx, rate,
      slots: leaderIdx >= 0 ? [slots[0], leaderIdx, slots[1]] : [0, 1, 2] };
    const lc = leaderIdx >= 0 ? info[leaderIdx].commit : Math.max(...info.map((x) => x.commit || 0));
    info.forEach((x, i) => {
      const c = cards[i], down = x.role === "down";
      c.g.setAttribute("class", "nc " + x.role);
      c.g.style.transform = `translate(${SLOT_X[slotOf(i)]}px, ${CY}px)`;
      c.role.textContent = x.role === "pre-candidate" ? "pre-vote" : x.role;
      c.term.textContent = down ? "—" : x.term;
      c.idx.textContent = down ? "—" : x.applied.toLocaleString("en-US");
      const behind = Math.max(0, lc - x.applied);
      c.lag.textContent = down ? "—" : behind ? `${behind}` : "0";
      c.blocks.forEach((b, k) => {
        const idx = lc - (BLOCKS - 1) + k;
        b.setAttribute("class", "blk" + (!down && idx > 0 && x.applied >= idx ? " on" : ""));
      });
    });
    for (const side of ["L", "R"]) {
      const f = st.slots[side === "L" ? 0 : 2];
      const ok = leaderIdx >= 0 && info[f].role !== "down";
      pipes[side].g.setAttribute("class", "pipe" + (ok ? " live" : leaderIdx >= 0 ? " broken" : ""));
      pipes[side].lbl.textContent = leaderIdx < 0 ? "no leader" : ok ? `replicating to n${f + 1}` : `n${f + 1} unreachable`;
    }
  }

  return { init, update };
})();
