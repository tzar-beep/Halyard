"use strict";
// Raft playground: Halyard's production raft package compiled to WebAssembly,
// driven tick by tick from here and drawn as SVG.
(() => {
  const root = document.getElementById("pg");
  if (!root) return;
  const $ = (s) => root.querySelector(s);
  const NS = "http://www.w3.org/2000/svg";
  const N = 5, CX = 260, CY = 215, R = 150;
  const TERM_COLORS = ["#8b949e", "#3fb950", "#6f8cff", "#d29922", "#c678dd", "#56b6c2", "#e06c75"];
  const MSG = {
    App: ["app", 5], AppResp: ["ack", 3.5], Heartbeat: ["hb", 3], HeartbeatResp: ["hbr", 2.5],
    Vote: ["vote", 5], VoteResp: ["voter", 3.5], PreVote: ["vote", 4], PreVoteResp: ["voter", 3], Snap: ["snap", 6], TimeoutNow: ["vote", 5],
  };
  const pos = [...Array(N)].map((_, i) => {
    const a = -Math.PI / 2 + (i * 2 * Math.PI) / N;
    return [CX + R * Math.cos(a), CY + R * Math.sin(a)];
  });

  let api = null, view = null, playing = true, speed = 8, acc = 0, last = performance.now(), frac = 0;
  let seed = Math.floor(Math.random() * 100000);
  let menuFor = 0;

  const svg = $("#pg-svg"), msgLayer = svg.querySelector(".pg-msgs"), nodeLayer = svg.querySelector(".pg-nodes");
  const el = (tag, a = {}, p) => { const e = document.createElementNS(NS, tag); for (const k in a) e.setAttribute(k, a[k]); if (p) p.appendChild(e); return e; };

  // Static node elements.
  const nodes = pos.map(([x, y], i) => {
    const g = el("g", { class: "pn", transform: `translate(${x},${y})`, tabindex: 0, role: "button", "aria-label": `node ${i + 1}` }, nodeLayer);
    el("circle", { r: 46, class: "pn-pulse" }, g);
    el("circle", { r: 36, class: "pn-body" }, g);
    const arc = el("circle", { r: 40, class: "pn-timer", pathLength: 100 }, g);
    const term = el("text", { class: "pn-term", y: 2 }, g);
    const sub = el("text", { class: "pn-sub", y: 18 }, g);
    const name = el("text", { class: "pn-name", y: 62 }, g); name.textContent = "n" + (i + 1);
    const tag = el("text", { class: "pn-tag", y: -52 }, g);
    g.addEventListener("click", (e) => { e.stopPropagation(); openMenu(i + 1, x, y); });
    g.addEventListener("keydown", (e) => { if (e.key === "Enter") openMenu(i + 1, x, y); });
    return { g, arc, term, sub, tag };
  });
  // Faint full mesh.
  const mesh = svg.querySelector(".pg-mesh");
  for (let a = 0; a < N; a++) for (let b = a + 1; b < N; b++)
    el("line", { x1: pos[a][0], y1: pos[a][1], x2: pos[b][0], y2: pos[b][1] }, mesh);
  const msgPool = [];
  let msgPos = [];

  function openMenu(id, x, y) {
    if (!view) return;
    const n = view.nodes[id - 1];
    menuFor = id;
    const m = $("#pg-menu");
    m.querySelector('[data-a="crash"]').textContent = n.up ? "Crash" : "Restart";
    m.querySelector('[data-a="isolate"]').textContent = n.isolated ? "Reconnect" : "Partition";
    m.querySelector(".pg-menu-title").textContent = `n${id} · ${n.state} · term ${n.term}`;
    const box = svg.getBoundingClientRect(), sx = box.width / 520, sy = box.height / 430;
    m.style.left = `${Math.min(x * sx + 24, box.width - 180)}px`;
    m.style.top = `${y * sy - 20}px`;
    m.hidden = false;
  }
  document.addEventListener("click", () => { $("#pg-menu").hidden = true; });
  $("#pg-menu").addEventListener("click", (e) => {
    e.stopPropagation();
    const a = e.target.dataset.a;
    if (!a || !api) return;
    const n = view.nodes[menuFor - 1];
    if (a === "crash") n.up ? api.crash(menuFor) : api.restart(menuFor);
    if (a === "isolate") api.isolate(menuFor);
    $("#pg-menu").hidden = true;
    refresh();
  });

  function refresh() {
    view = JSON.parse(api.view());
    view.msgs = view.msgs || [];
    view.nodes.forEach((n) => { n.log = n.log || []; });
    feed(view.events || []);
    drawLog();
  }

  function feed(events) {
    const ol = $("#pg-feed");
    for (const ev of events) {
      const li = document.createElement("li");
      li.className = ev.kind;
      li.innerHTML = `<span class="t">${String(ev.tick).padStart(4, "0")}</span><span></span>`;
      li.lastChild.textContent = ev.text;
      ol.prepend(li);
    }
    while (ol.children.length > 60) ol.lastChild.remove();
  }

  function drawNodes() {
    view.nodes.forEach((n, i) => {
      const o = nodes[i];
      const state = n.isolated && n.up ? `${n.state} isolated` : n.state;
      o.g.setAttribute("class", "pn " + state);
      o.term.textContent = n.up ? n.term : "×";
      o.sub.textContent = n.up ? `#${n.commit}` : "down";
      o.tag.textContent = n.isolated ? "partitioned" : n.state === "leader" ? "leader" : n.state === "candidate" ? "candidate" : "";
      const p = n.up && n.state !== "leader" ? Math.min(1, (n.elapsed + frac) / Math.max(1, n.timeout)) : 0;
      o.arc.setAttribute("stroke-dasharray", `${(p * 100).toFixed(1)} 100`);
      o.arc.classList.toggle("hot", p > 0.75);
    });
  }

  function drawMsgs() {
    const t = view.tick + frac;
    let k = 0;
    msgPos = [];
    for (const m of view.msgs) {
      const spec = MSG[m.type] || ["hb", 3];
      const p = Math.min(1, Math.max(0, (t - m.sent) / Math.max(1, m.due - m.sent)));
      const [x1, y1] = pos[m.from - 1], [x2, y2] = pos[m.to - 1];
      const c = msgPool[k] || (msgPool[k] = el("circle", {}, msgLayer));
      c.setAttribute("class", `pm ${spec[0]}${m.fail ? " fail" : ""}`);
      c.setAttribute("r", spec[1]);
      const mx = x1 + (x2 - x1) * p, my = y1 + (y2 - y1) * p;
      msgPos.push({ m, x: mx, y: my });
      c.setAttribute("cx", mx.toFixed(1));
      c.setAttribute("cy", my.toFixed(1));
      c.style.display = "";
      k++;
    }
    for (; k < msgPool.length; k++) msgPool[k].style.display = "none";
  }

  function drawLog() {
    const last = Math.max(1, ...view.nodes.map((n) => n.last));
    const cols = 22, from = Math.max(1, last - cols + 1);
    let h = `<div class="lg-row lg-head"><span></span>`;
    for (let i = from; i < from + cols; i++) h += `<span>${i}</span>`;
    h += "</div>";
    for (const n of view.nodes) {
      const byIdx = new Map(n.log.map((e) => [e.i, e]));
      h += `<div class="lg-row ${n.up ? "" : "down"}"><span class="lg-name">n${n.id}</span>`;
      for (let i = from; i < from + cols; i++) {
        const e = byIdx.get(i);
        if (!e) { h += "<span></span>"; continue; }
        const col = TERM_COLORS[e.t % TERM_COLORS.length];
        const committed = i <= n.commit;
        h += `<span class="c ${committed ? "ok" : "un"}" style="--c:${col}" title="index ${i} · term ${e.t} · ${e.d}${committed ? "" : " (uncommitted)"}">${e.t}</span>`;
      }
      h += "</div>";
    }
    $("#pg-log").innerHTML = h;
    $("#pg-tick").textContent = view.tick;
    $("#pg-seed-now").textContent = view.seed;
    const l = view.leader ? `n${view.leader} · term ${view.nodes[view.leader - 1].term}` : "electing…";
    $("#pg-leader").textContent = l;
  }

  // Simulation advances on a timer (keeps running even when frames are throttled);
  // drawing happens on animation frames, interpolating message positions.
  let lastStep = performance.now();
  setInterval(() => {
    const now = performance.now();
    const dt = Math.min(0.25, (now - lastStep) / 1000);
    lastStep = now;
    if (!api || !playing) return;
    acc += dt * speed;
    let steps = 0;
    while (acc >= 1 && steps < 8) { acc -= 1; steps++; }
    if (steps) { api.step(steps); refresh(); runScenario(); }
  }, 30);

  function frame() {
    if (api && playing) frac = Math.min(0.999, acc + (performance.now() - lastStep) / 1000 * speed);
    if (view) { drawNodes(); drawMsgs(); }
    requestAnimationFrame(frame);
  }

  function reset(newSeed) {
    seed = newSeed;
    api.create(N, seed);
    $("#pg-feed").innerHTML = "";
    $("#pg-seed").value = seed;
    acc = 0;
    refresh();
  }

  // Controls.
  $("#pg-play").addEventListener("click", () => { playing = !playing; $("#pg-play").textContent = playing ? "Pause" : "Play"; });
  $("#pg-step").addEventListener("click", () => { if (!api) return; playing = false; $("#pg-play").textContent = "Play"; api.step(1); frac = 0; refresh(); });
  $("#pg-speed").addEventListener("input", (e) => { speed = [2, 4, 8, 16, 32][e.target.value]; $("#pg-speed-v").textContent = `${speed} ticks/s`; });
  $("#pg-write").addEventListener("click", () => { if (api) { api.propose(""); refresh(); } });
  $("#pg-kill").addEventListener("click", () => { if (api && view.leader) { api.crash(view.leader); refresh(); } });
  $("#pg-loss").addEventListener("input", (e) => { $("#pg-loss-v").textContent = `${e.target.value}%`; if (api) api.loss(e.target.value / 100); });
  $("#pg-reset").addEventListener("click", () => reset(parseInt($("#pg-seed").value, 10) || 1));
  $("#pg-random").addEventListener("click", () => reset(Math.floor(Math.random() * 100000)));


  // ---------- hover tooltips: what is inside each RPC ----------
  const tip = $("#pg-tip");
  const describe = (m) => {
    const dir = `n${m.from} → n${m.to}`;
    const drop = m.fail ? " · will be dropped" : "";
    switch (m.type) {
      case "App": return [`AppendEntries ${dir}`, `term ${m.term} · prev ${m.index || 0}@${m.logTerm || 0} · ${m.entries || 0} entr${m.entries === 1 ? "y" : "ies"} · leader commit ${m.commit || 0}${drop}`];
      case "AppResp": return [`AppendEntries ${m.reject ? "rejected" : "ack"} ${dir}`, m.reject ? `term ${m.term} · conflict near index ${m.index || 0}${drop}` : `term ${m.term} · replicated up to ${m.index || 0}${drop}`];
      case "Heartbeat": return [`Heartbeat ${dir}`, `term ${m.term} · commit ${m.commit || 0}${drop}`];
      case "HeartbeatResp": return [`Heartbeat ack ${dir}`, `term ${m.term}${drop}`];
      case "PreVote": case "Vote": return [`${m.type === "PreVote" ? "PreVote" : "RequestVote"} ${dir}`, `term ${m.term} · candidate's last log ${m.index || 0}@${m.logTerm || 0}${drop}`];
      case "PreVoteResp": case "VoteResp": return [`${m.reject ? "Vote denied" : "Vote granted"} ${dir}`, `term ${m.term}${drop}`];
      case "Snap": return [`InstallSnapshot ${dir}`, `term ${m.term}${drop}`];
      default: return [`${m.type} ${dir}`, `term ${m.term}${drop}`];
    }
  };
  const pointer = (e) => {
    const b = svg.getBoundingClientRect();
    return [((e.clientX - b.left) / b.width) * 520, ((e.clientY - b.top) / b.height) * 430, e.clientX - b.left, e.clientY - b.top];
  };
  svg.addEventListener("mousemove", (e) => {
    if (!view) return;
    const [x, y, px, py] = pointer(e);
    let best = null, bd = 14;
    for (const p of msgPos) { const d = Math.hypot(p.x - x, p.y - y); if (d < bd) { bd = d; best = p.m; } }
    let lines = null;
    if (best) lines = describe(best);
    else {
      const i = pos.findIndex(([nx, ny]) => Math.hypot(nx - x, ny - y) < 40);
      if (i >= 0) {
        const n = view.nodes[i];
        lines = n.up
          ? [`n${n.id} · ${n.state}${n.isolated ? " · partitioned" : ""}`, `term ${n.term}${n.vote ? ` · voted n${n.vote}` : ""} · log up to ${n.last} · commit ${n.commit}${n.state !== "leader" ? ` · timer ${n.elapsed}/${n.timeout}` : ""}`]
          : [`n${n.id} · down`, "crashed — click to restart from disk"];
      }
    }
    if (!lines) { tip.hidden = true; return; }
    tip.innerHTML = `<b></b><span></span>`;
    tip.firstChild.textContent = lines[0];
    tip.lastChild.textContent = lines[1];
    tip.hidden = false;
    const b = svg.getBoundingClientRect();
    tip.style.left = `${Math.min(px + 14, b.width - 280)}px`;
    tip.style.top = `${Math.max(8, py - 54)}px`;
  });
  svg.addEventListener("mouseleave", () => { tip.hidden = true; });

  // ---------- auto-played scenario ----------
  // Runs on load so the cluster tells its own story; any user action hands control over.
  let scenario = null;
  const banner = $("#pg-scenario");
  function startScenario() {
    scenario = { phase: 0, at: 0, crashed: 0, isolated: 0 };
    banner.hidden = false;
  }
  function stopScenario() {
    if (!scenario) return;
    scenario = null;
    banner.hidden = true;
  }
  function say(text) { banner.querySelector("span").textContent = text; }
  function runScenario() {
    if (!scenario || !view) return;
    const s = scenario, t = view.tick, L = view.leader;
    const next = (delay) => { s.phase++; s.at = t + delay; };
    if (t < s.at) return;
    switch (s.phase) {
      case 0: say("Five nodes start as followers. Watch the election timers fill up…"); if (L) next(10); break;
      case 1: say(`n${L} won the election. A client writes; the leader replicates the entry to a majority.`); api.propose(""); next(10); break;
      case 2: api.propose(""); next(10); break;
      case 3: api.propose(""); say("Each entry turns solid in the log grid once a majority has it — that is the commit point."); next(36); break;
      case 4: s.crashed = L; api.crash(L); say(`Now the leader n${L} crashes. Followers stop hearing heartbeats…`); next(4); break;
      case 5: if (L && L !== s.crashed) { say(`n${L} won a new election in term ${view.nodes[L - 1].term}. No committed entry was lost.`); next(28); } break;
      case 6: api.propose(""); api.restart(s.crashed); say(`n${s.crashed} restarts from its disk and catches up from the new leader.`); next(50); break;
      case 7: {
        const f = view.nodes.find((n) => n.up && n.id !== L && n.id !== s.crashed);
        if (f) { s.isolated = f.id; api.isolate(f.id); say(`n${f.id} is cut off by a network partition. The majority keeps committing without it.`); }
        next(8); break;
      }
      case 8: api.propose(""); next(12); break;
      case 9: api.propose(""); next(40); break;
      case 10: api.isolate(s.isolated); say(`The partition heals; n${s.isolated} rejoins and its log is repaired. Your turn — click any node.`); next(70); break;
      case 11: stopScenario(); break;
    }
    refresh();
  }
  banner.querySelector("button").addEventListener("click", stopScenario);
  // Any manual control ends the scenario.
  root.querySelector(".pg-bar").addEventListener("click", (e) => { if (e.target.closest("button")) stopScenario(); }, true);
  $("#pg-menu").addEventListener("click", stopScenario, true);

  // Load the wasm module.
  addEventListener("halyard-ready", () => {
    api = window.halyardRaft;
    root.classList.add("ready");
    reset(seed);
    startScenario();
  }, { once: true });
  const go = new Go();
  WebAssembly.instantiateStreaming(fetch("raft.wasm"), go.importObject)
    .then((r) => go.run(r.instance))
    .catch(() => { $("#pg-loading").textContent = "Could not load the WebAssembly module."; });
  requestAnimationFrame(frame);
})();
