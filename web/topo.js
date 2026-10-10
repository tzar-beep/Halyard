"use strict";
// Animated replication topology: nodes, curved links, and packets (heartbeats,
// appends, acks) moving along them. Built once; update() only changes state.
window.Topo = (() => {
  const NS = "http://www.w3.org/2000/svg";
  const W = 720, H = 420;
  const POS = [[360, 92], [170, 318], [550, 318]];
  const BLOCKS = 12;
  let svg, edgeLayer, packetLayer, nodeEls = [], edges = {}, packets = [], pool = [];
  let state = { roles: [], leader: -1, rate: 0 };
  let lastBeat = 0, appendAcc = 0, last = performance.now();

  const el = (tag, attrs = {}, parent) => {
    const e = document.createElementNS(NS, tag);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  };

  // Quadratic curve between two nodes, bowed away from the centre.
  function curve(a, b) {
    const [x1, y1] = POS[a], [x2, y2] = POS[b];
    const mx = (x1 + x2) / 2, my = (y1 + y2) / 2;
    const cx = W / 2, cy = (POS[0][1] + POS[1][1] * 2) / 3;
    const dx = mx - cx, dy = my - cy, len = Math.hypot(dx, dy) || 1;
    return { x1, y1, x2, y2, qx: mx + (dx / len) * 34, qy: my + (dy / len) * 34 };
  }
  const at = (c, t) => {
    const u = 1 - t;
    return [u * u * c.x1 + 2 * u * t * c.qx + t * t * c.x2, u * u * c.y1 + 2 * u * t * c.qy + t * t * c.y2];
  };

  function init(svgEl, names) {
    svg = svgEl;
    svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
    const defs = el("defs", {}, svg);
    const pat = el("pattern", { id: "dots", width: 18, height: 18, patternUnits: "userSpaceOnUse" }, defs);
    el("circle", { cx: 1, cy: 1, r: 1, class: "bgdot" }, pat);
    const glow = el("filter", { id: "glow", x: "-50%", y: "-50%", width: "200%", height: "200%" }, defs);
    el("feGaussianBlur", { stdDeviation: 6, result: "b" }, glow);
    const m = el("feMerge", {}, glow);
    el("feMergeNode", { in: "b" }, m); el("feMergeNode", { in: "SourceGraphic" }, m);
    const pg = el("filter", { id: "pglow", x: "-200%", y: "-200%", width: "500%", height: "500%" }, defs);
    el("feGaussianBlur", { stdDeviation: 2.5, result: "b" }, pg);
    const m2 = el("feMerge", {}, pg);
    el("feMergeNode", { in: "b" }, m2); el("feMergeNode", { in: "SourceGraphic" }, m2);
    el("rect", { width: W, height: H, fill: "url(#dots)" }, svg);

    edgeLayer = el("g", {}, svg);
    for (let a = 0; a < 3; a++) for (let b = a + 1; b < 3; b++) {
      const c = curve(a, b);
      const d = `M${c.x1} ${c.y1} Q${c.qx} ${c.qy} ${c.x2} ${c.y2}`;
      edges[a + "-" + b] = { base: el("path", { d, class: "edge" }, edgeLayer), live: el("path", { d, class: "edge-live" }, edgeLayer) };
    }
    packetLayer = el("g", {}, svg);

    nodeEls = names.map((name, i) => {
      const g = el("g", { class: "tn follower", transform: `translate(${POS[i][0]},${POS[i][1]})` }, svg);
      el("circle", { r: 70, class: "halo" }, g);
      el("circle", { r: 70, class: "ripple" }, g);
      el("rect", { x: -92, y: -50, width: 184, height: 100, rx: 14, class: "card" }, g);
      // server glyph
      const ic = el("g", { transform: "translate(-74,-34)", class: "glyph" }, g);
      for (let k = 0; k < 3; k++) {
        el("rect", { x: 0, y: k * 9, width: 22, height: 7, rx: 2 }, ic);
        el("circle", { cx: 18, cy: k * 9 + 3.5, r: 1.3, class: "led" }, ic);
      }
      const name_ = el("text", { x: -42, y: -20, class: "tn-name" }, g); name_.textContent = name;
      const badge = el("g", { transform: "translate(40,-27)" }, g);
      el("rect", { x: -32, y: -10, width: 72, height: 20, rx: 10, class: "badge" }, badge);
      const role = el("text", { x: 4, y: 4, class: "tn-role", "text-anchor": "middle" }, badge);
      const meta = el("text", { x: -74, y: 10, class: "tn-meta" }, g);
      const blocks = [];
      for (let k = 0; k < BLOCKS; k++) blocks.push(el("rect", { x: -74 + k * 12.6, y: 24, width: 9.6, height: 12, rx: 2, class: "blk" }, g));
      const lag = el("text", { x: 74, y: 10, class: "tn-lag", "text-anchor": "end" }, g);
      return { g, role, meta, blocks, lag };
    });
    requestAnimationFrame(frame);
  }

  function spawn(from, to, kind) {
    if (from === to) return;
    const key = Math.min(from, to) + "-" + Math.max(from, to);
    const c = curve(Math.min(from, to), Math.max(from, to));
    const dot = pool.pop() || el("circle", {}, packetLayer);
    dot.setAttribute("class", "pkt " + kind);
    dot.setAttribute("r", kind === "append" ? 4 : kind === "beat" ? 3 : 2.4);
    dot.setAttribute("filter", "url(#pglow)");
    dot.style.display = "";
    packets.push({ c, rev: from > to, t: 0, speed: kind === "ack" ? 1.4 : 1.1, kind, dot, from, to, key });
  }

  function frame(now) {
    const dt = Math.min(0.05, (now - last) / 1000);
    last = now;
    const { roles, leader, rate } = state;
    if (leader >= 0) {
      const live = [0, 1, 2].filter((i) => i !== leader && roles[i] !== "down");
      if (now - lastBeat > 650) { lastBeat = now; live.forEach((f) => spawn(leader, f, "beat")); }
      appendAcc += Math.min(rate, 10) * dt;
      while (appendAcc >= 1) { appendAcc -= 1; live.forEach((f) => spawn(leader, f, "append")); }
    }
    packets = packets.filter((p) => {
      p.t += p.speed * dt;
      if (p.t >= 1) {
        p.dot.style.display = "none";
        pool.push(p.dot);
        if (p.kind !== "ack" && roles[p.to] !== "down") spawn(p.to, p.from, "ack");
        return false;
      }
      const [x, y] = at(p.c, p.rev ? 1 - p.t : p.t);
      p.dot.setAttribute("cx", x.toFixed(1));
      p.dot.setAttribute("cy", y.toFixed(1));
      return true;
    });
    requestAnimationFrame(frame);
  }

  // info[i] = { role, term, commit, applied }
  function update(info, leaderIdx, rate) {
    state = { roles: info.map((x) => x.role), leader: leaderIdx, rate };
    const lc = leaderIdx >= 0 ? info[leaderIdx].commit : Math.max(...info.map((x) => x.commit || 0));
    info.forEach((x, i) => {
      const n = nodeEls[i];
      n.g.setAttribute("class", "tn " + x.role);
      n.role.textContent = x.role;
      const down = x.role === "down";
      n.meta.textContent = down ? "no heartbeat" : `T${x.term} · #${x.applied}`;
      const behind = down ? null : Math.max(0, lc - x.applied);
      n.lag.textContent = down ? "" : behind ? `−${behind}` : "in sync";
      n.blocks.forEach((b, k) => {
        const idx = lc - (BLOCKS - 1) + k;
        b.setAttribute("class", "blk" + (!down && idx > 0 && x.applied >= idx ? " on" : ""));
      });
    });
    for (const key in edges) {
      const [a, b] = key.split("-").map(Number);
      const active = leaderIdx >= 0 && (a === leaderIdx || b === leaderIdx) && info[a].role !== "down" && info[b].role !== "down";
      edges[key].live.setAttribute("class", "edge-live" + (active ? " on" : ""));
    }
  }

  return { init, update };
})();
