'use strict';
// Batch-job partitions in this browser tab.
//
// A tab that shares its processor also takes partitions of jobs the engine's own operators
// can run: totals and group-bys over JSON Lines, CSV or text. It never runs a job's code: a
// friend's browser gets only these operators, which come with the page. Each partition runs
// in a Web Worker (engine-worker.js) and gives exactly the state a computer would, or is
// handed back when the tab can't be sure it would (a regular expression, digits from another
// script, ...).
//
// Before offering anything the tab measures itself once, briefly: a small benchmark, its
// cores and memory (when the browser says), its battery (when it says), and whether it has
// what the workers need. It offers no partitions while hidden (a locked phone, a background
// tab), on a low battery that isn't charging, or when sharing is off; partitions running
// when the tab is hidden are handed back at once so other computers take them.
(() => {
  const OPS = ['batch.aggregate', 'batch.groupby'];
  const H = {
    send: () => {}, sendText: () => {}, sharing: () => false, threads: () => 1,
    running: new Map(), unacked: new Map(), profile: null, battery: null,
  };
  const key = (p, n) => `${p}\u0000${n}`;

  async function measure() {
    const p = { ok: false, why: '', cores: navigator.hardwareConcurrency || null, memoryGB: navigator.deviceMemory || null,
      battery: 'unknown', recordsPerSecond: null, workers: typeof Worker !== 'undefined', at: Date.now() / 1000 };
    const ops = window.YHEngineOps;
    const needs = [[typeof BigInt === 'function', 'big integers'], [p.workers, 'Web Workers'], [typeof TextDecoder === 'function', 'text decoding'],
      [typeof ReadableStream === 'function', 'streamed downloads'], [!!ops, 'the batch operators']];
    const missing = needs.filter(([have]) => !have).map(([, what]) => what);
    if (missing.length) { p.why = `This browser lacks ${missing.join(', ')}.`; return p; }
    try { // about 4,000 records: long enough to tell a phone from a desktop, short enough not to notice
      const lines = [];
      for (let i = 0; i < 4000; i++) lines.push(`{"g":${i % 9},"x":${(i * 7919) % 1000 / 8},"q":${i % 13}}`);
      const data = new TextEncoder().encode(`${lines.join('\n')}\n`);
      const op = ops.makeOp({ type: 'batch.groupby', by: ['g'], where: [], metrics: [{ op: 'count' }, { op: 'sum', field: 'x' }, { op: 'max', field: 'q' }] });
      const t0 = performance.now();
      ops.runPartition(op, 'jsonl', null, [data], 0);
      const ms = Math.max(1, performance.now() - t0);
      p.recordsPerSecond = Math.round(4000 / (ms / 1000));
      p.bytesPerSecond = Math.round(data.length / (ms / 1000));
    } catch (e) { p.why = `The batch operators don't work here (${e.message}).`; return p; }
    try {
      if (navigator.getBattery) {
        H.battery = await navigator.getBattery();
        const sync = () => { p.battery = { charging: H.battery.charging, level: Math.round(H.battery.level * 100) / 100 }; };
        sync();
        H.battery.addEventListener('levelchange', () => { sync(); update(); });
        H.battery.addEventListener('chargingchange', () => { sync(); update(); });
      }
    } catch { /* the battery stays unknown */ }
    p.ok = true;
    return p;
  }
  const ready = measure().then((p) => { H.profile = p; return p; });

  function blocked() {
    const p = H.profile;
    if (!p) return 'still measuring this browser';
    if (!p.ok) return p.why;
    if (!H.sharing()) return 'sharing is off';
    if (document.visibilityState === 'hidden') return 'the page is hidden';
    if (p.battery && p.battery !== 'unknown' && !p.battery.charging && p.battery.level < 0.2) return 'the battery is low';
    return '';
  }
  function slots() { return blocked() ? 0 : Math.max(1, Math.min(H.threads(), 4)); }

  function describe() {
    const running = [...H.running.values(), ...H.unacked.values()].map((r) => ({ partition: r.partition, attempt: r.attempt }));
    return { slots: slots(), ops: OPS, formats: ['jsonl', 'csv', 'text'], runtimes: ['builtin'], stages: ['map'], unsupported: ['where.regex'],
      python: '', running, profile: H.profile ? { ...H.profile, blocked: blocked() } : null };
  }
  function update() { H.send({ type: 'update', engine: describe() }); }

  function start(m) {
    const k = key(m.partition, m.attempt);
    if (H.running.has(k)) return;
    const refuse = (reason) => H.send({ type: 'eng.refuse', partition: m.partition, attempt: m.attempt, reason });
    if (!m.taskJSON) return refuse('This coordinator sends partitions in a form this page doesn’t read; reload it.');
    if (H.running.size >= slots()) return refuse(blocked() ? `This browser tab can't take partitions now: ${blocked()}.` : 'This browser tab is busy.');
    const w = new Worker('engine-worker.js');
    const r = { partition: m.partition, attempt: m.attempt, worker: w, records: null };
    r.beat = setInterval(() => H.send({ type: 'eng.progress', partition: m.partition, attempt: m.attempt, records: r.records }), 2000);
    H.running.set(k, r);
    w.onmessage = (e) => {
      if (e.data.type === 'progress') r.records = e.data.records;
      else if (e.data.type === 'done') finish(k, e.data.text);
    };
    w.onerror = (e) => finish(k, JSON.stringify({ type: 'eng.done', partition: m.partition, attempt: m.attempt, ok: false, errorKind: 'infra',
      error: `The browser's worker stopped: ${e.message || 'unknown error'}` }));
    w.postMessage({ taskJSON: m.taskJSON, partition: m.partition, attempt: m.attempt, token: m.token });
    H.send({ type: 'eng.progress', partition: m.partition, attempt: m.attempt, records: 0 });
  }

  function stop(r) {
    clearInterval(r.beat);
    try { r.worker.postMessage({ type: 'stop' }); r.worker.terminate(); } catch { /* gone */ }
  }

  function finish(k, text) {
    const r = H.running.get(k);
    if (!r) return;
    stop(r);
    H.running.delete(k);
    H.unacked.set(k, { partition: r.partition, attempt: r.attempt, text }); // kept until the coordinator acknowledges it
    H.sendText(text);
  }

  // Hand back everything running, without counting it as a failed try, and offer nothing until it can again.
  function release(why) {
    for (const [k, r] of H.running) {
      stop(r);
      H.running.delete(k);
      H.send({ type: 'eng.release', partition: r.partition, attempt: r.attempt, reason: why });
    }
    update();
  }

  function handle(m) {
    const k = key(m.partition, m.attempt);
    if (m.type === 'eng.start') start(m);
    else if (m.type === 'eng.cancel') {
      const r = H.running.get(k);
      if (r) { stop(r); H.running.delete(k); }
      H.unacked.delete(k);
    } else if (m.type === 'eng.ack') H.unacked.delete(k);
  }

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden' && H.running.size) release('the page was hidden');
    else update();
  });

  window.YHEngine = {
    attach(opts) { Object.assign(H, opts); },
    ready: () => ready,
    describe,
    handle,
    connected() { for (const u of H.unacked.values()) H.sendText(u.text); update(); },
    sharingChanged() { if (!H.sharing() && H.running.size) release('sharing was turned off'); else update(); },
    get running() { return H.running.size; },
  };
})();
