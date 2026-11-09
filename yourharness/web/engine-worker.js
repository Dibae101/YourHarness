'use strict';
// One batch-job partition in a browser tab, in a Web Worker so the page stays responsive.
// It streams the partition's bytes from the coordinator with the attempt's own token,
// runs the engine's built-in operator on them (engine-ops.js, the same rules as the
// Python workers), and posts back the eng.done message text. The state goes in as text,
// never re-parsed, so a Python float like 5.0 keeps its decimal point on the way.
importScripts('engine-ops.js');
const O = self.YHEngineOps;
const INLINE_STATE = 256 * 1024;
let stopped = false;

function kindOf(err) {
  if (err instanceof O.Unsupported) return 'unsupported';
  if (err instanceof O.RecordError || err instanceof O.OpError) return 'data';
  return 'infra';
}

self.onmessage = async (e) => {
  const m = e.data;
  if (m.type === 'stop') { stopped = true; return; }
  const head = `{"type":"eng.done","partition":${JSON.stringify(m.partition)},"attempt":${Number(m.attempt)}`;
  const t0 = performance.now();
  const auth = { Authorization: `Bearer ${m.token}` };
  const base = `/api/engine/attempts/${encodeURIComponent(m.partition)}/${Number(m.attempt)}`;
  try {
    const task = O.parseJSON(m.taskJSON);
    const op = O.check(task); // anything a browser can't do exactly like a computer stops here
    const start = task.input.kind === 'range' ? Number(task.input.start) : 0;
    const run = new O.PartitionRun(op, task.format, task.header, start, task.onBadRecord || 'fail');
    const res = await fetch(`${base}/input`, { headers: auth, cache: 'no-store' });
    if (!res.ok) throw new Error(`the coordinator answered ${res.status} for the input`);
    const reader = res.body.getReader();
    let bytes = 0;
    let beat = performance.now();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (stopped) { reader.cancel(); return; }
      bytes += value.length;
      run.push(value);
      if (performance.now() - beat > 1000) { beat = performance.now(); self.postMessage({ type: 'progress', records: run.n }); }
    }
    const stats = run.end();
    const state = O.stateJSON(op.state());
    let tail;
    if (state.length > INLINE_STATE) {
      const up = await fetch(`${base}/outputs/state.json`, { method: 'PUT', headers: { ...auth, 'Content-Type': 'application/octet-stream' }, body: state });
      if (!up.ok) throw new Error(`the coordinator answered ${up.status} for the state`);
      const got = await up.json();
      tail = `,"stateOutput":"state.json","outputs":{"state.json":{"sha":${JSON.stringify(got.sha)},"size":${Number(got.size)}}}`;
    } else {
      tail = `,"state":${state},"outputs":{}`;
    }
    const seconds = Math.round(performance.now() - t0) / 1000;
    self.postMessage({ type: 'done', text: `${head},"ok":true,"records":${stats.records},"skipped":${stats.skipped},"bad":${JSON.stringify(stats.bad)},`
      + `"seconds":${seconds},"bytes":${bytes}${tail}}` });
  } catch (err) {
    if (stopped) return;
    self.postMessage({ type: 'done', text: `${head},"ok":false,"errorKind":${JSON.stringify(kindOf(err))},"error":${JSON.stringify(String(err && err.message || err).slice(0, 2000))}}` });
  }
};
