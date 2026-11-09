// Runs the browser's batch operators (yourharness/web/engine-ops.js) on cases read from stdin,
// one JSON object per line, and prints each partition's state as the coordinator would receive it.
// tests/test_browser_ops.py compares these with the Python operators on the same bytes.
import fs from 'node:fs';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
require('../../yourharness/web/engine-ops.js');
const O = globalThis.YHEngineOps;

function cut(data, cuts) {
  const out = [];
  let at = 0;
  for (const c of [...cuts, data.length]) {
    if (c > at) out.push(new Uint8Array(data.subarray(at, Math.min(c, data.length))));
    at = Math.max(at, c);
  }
  return out;
}

const out = [];
for (const line of fs.readFileSync(0, 'utf8').split('\n')) {
  if (!line) continue;
  const c = JSON.parse(line);
  const data = Buffer.from(c.data, 'base64');
  try {
    const op = O.makeOp(O.parseJSON(c.wl));
    const stats = O.runPartition(op, c.format, c.header, cut(data, c.cuts), c.start, c.bad);
    out.push(`{"ok":true,"state":${O.stateJSON(op.state())},"records":${stats.records},"skipped":${stats.skipped},`
      + `"badOffsets":${JSON.stringify(stats.bad.map((b) => b.offset))}}`);
  } catch (e) {
    if (e instanceof O.Unsupported) out.push(JSON.stringify({ ok: false, kind: 'unsupported', error: e.message }));
    else if (e instanceof O.RecordError) out.push(JSON.stringify({ ok: false, kind: 'data', offset: e.offset, error: e.message }));
    else if (e instanceof O.OpError) out.push(JSON.stringify({ ok: false, kind: 'op', error: e.message }));
    else out.push(JSON.stringify({ ok: false, kind: 'crash', error: String((e && e.stack) || e) }));
  }
}
process.stdout.write(`${out.join('\n')}\n`);
