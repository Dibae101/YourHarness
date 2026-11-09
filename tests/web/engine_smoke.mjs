// Headless smoke test of batch partitions in a browser tab: the host (yourharness/web/engine.js)
// in jsdom with stand-in Web Workers, and the worker itself (yourharness/web/engine-worker.js)
// in this process with a stubbed coordinator. tests/test_browser_ops.py checks the operators'
// results against Python; this checks what the tab offers, hands back and sends.
// Needs jsdom:  npm install --no-save jsdom@26   then:  node tests/web/engine_smoke.mjs
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import assert from 'node:assert/strict';

const web = new URL('../../yourharness/web/', import.meta.url);
const read = (name) => fs.readFileSync(new URL(name, web), 'utf8');
const last = (msgs, type) => msgs.filter((m) => m.type === type).at(-1);
const plain = (v) => (v === undefined ? v : JSON.parse(JSON.stringify(v))); // values made in the page's realm, comparable here

// ---------------------------------------------------------------- engine.js in a page
function tab({ battery = null, workers = true } = {}) {
  const dom = new JSDOM('<!doctype html><body></body>', { url: 'http://127.0.0.1:8420/', runScripts: 'outside-only' });
  const { window } = dom;
  Object.assign(window, { TextEncoder, TextDecoder, ReadableStream });
  let hidden = false;
  Object.defineProperty(window.document, 'visibilityState', { get: () => (hidden ? 'hidden' : 'visible'), configurable: true });
  const made = [];
  if (workers) {
    window.Worker = class {
      constructor(url) { this.url = url; this.posted = []; this.terminated = false; made.push(this); }
      postMessage(m) { this.posted.push(plain(m)); }
      terminate() { this.terminated = true; }
    };
  }
  if (battery) Object.defineProperty(window.navigator, 'getBattery', { value: async () => battery, configurable: true });
  window.eval(read('engine-ops.js'));
  window.eval(read('engine.js'));
  const t = { window, E: window.YHEngine, sent: [], texts: [], workers: made, sharing: true };
  t.E.attach({ send: (m) => t.sent.push(plain(m)), sendText: (s) => t.texts.push(s), sharing: () => t.sharing, threads: () => 2 });
  t.describe = () => plain(window.YHEngine.describe());
  t.hide = (h) => { hidden = h; window.document.dispatchEvent(new window.Event('visibilitychange')); };
  t.share = (s) => { t.sharing = s; t.E.sharingChanged(); };
  t.start = (partition, attempt = 1) => t.E.handle({ type: 'eng.start', partition, attempt, token: `tok-${partition}`, taskJSON: '{"x":1}' });
  return t;
}

{
  const t = tab();
  assert.equal(t.describe().slots, 0, 'nothing is offered while the tab is still measuring itself');
  assert.equal(t.describe().profile, null);
  const p = plain(await t.E.ready());
  assert.equal(p.ok, true, p.why);
  assert.ok(p.recordsPerSecond > 0 && p.bytesPerSecond > 0, 'the benchmark ran');
  assert.equal(p.battery, 'unknown', 'a browser that does not say keeps its battery unknown');
  const d = t.describe();
  assert.equal(d.slots, 2);
  assert.equal(d.profile.blocked, '');
  assert.deepEqual(d.ops, ['batch.aggregate', 'batch.groupby']);
  assert.deepEqual(d.runtimes, ['builtin'], 'a browser never runs code a job brings');
  assert.deepEqual(d.unsupported, ['where.regex']);

  // a coordinator that sends the task as an object, not as JSON text, is refused rather than misread
  t.E.handle({ type: 'eng.start', partition: 'old', attempt: 1, token: 'x', task: { workload: {} } });
  assert.match(last(t.sent, 'eng.refuse').reason, /reload/);
  assert.equal(t.workers.length, 0);

  // two partitions run in workers; a third waits for a free slot
  t.start('j1:1');
  t.start('j1:2');
  t.start('j1:3');
  t.start('j1:1'); // the same attempt again: already running
  assert.equal(t.workers.length, 2);
  assert.equal(t.workers[0].url, 'engine-worker.js');
  assert.deepEqual(t.workers[0].posted[0], { taskJSON: '{"x":1}', partition: 'j1:1', attempt: 1, token: 'tok-j1:1' });
  assert.deepEqual(last(t.sent, 'eng.refuse'), { type: 'eng.refuse', partition: 'j1:3', attempt: 1, reason: 'This browser tab is busy.' });
  assert.ok(t.sent.some((m) => m.type === 'eng.progress' && m.partition === 'j1:2' && m.records === 0));

  // a finished partition's text goes out unchanged, and again after a reconnect until the coordinator acknowledges it
  const doneText = '{"type":"eng.done","partition":"j1:1","attempt":1,"ok":true,"records":5,"state":{"s":5.0},"outputs":{}}';
  t.workers[0].onmessage({ data: { type: 'progress', records: 3 } });
  t.workers[0].onmessage({ data: { type: 'done', text: doneText } });
  assert.deepEqual(t.texts, [doneText]);
  assert.equal(t.workers[0].terminated, true);
  assert.equal(t.E.running, 1);
  assert.deepEqual(t.describe().running.map((r) => r.partition).sort(), ['j1:1', 'j1:2'], 'a result not yet acknowledged still counts as held');
  t.E.connected();
  assert.deepEqual(t.texts, [doneText, doneText]);
  t.E.handle({ type: 'eng.ack', partition: 'j1:1', attempt: 1 });
  t.E.connected();
  assert.equal(t.texts.length, 2);
  assert.deepEqual(t.describe().running.map((r) => r.partition), ['j1:2']);

  // hidden: what runs is handed back at once, nothing is offered, and new work is refused
  t.hide(true);
  assert.deepEqual(t.sent.filter((m) => m.type === 'eng.release'), [{ type: 'eng.release', partition: 'j1:2', attempt: 1, reason: 'the page was hidden' }]);
  assert.equal(t.workers[1].terminated, true);
  assert.equal(t.E.running, 0);
  let up = last(t.sent, 'update').engine;
  assert.equal(up.slots, 0);
  assert.equal(up.profile.blocked, 'the page is hidden');
  assert.deepEqual(up.running, []);
  t.start('j1:4');
  assert.match(last(t.sent, 'eng.refuse').reason, /can't take partitions now: the page is hidden/);
  assert.equal(t.workers.length, 2);
  t.hide(false);
  assert.equal(last(t.sent, 'update').engine.slots, 2, 'visible again: it offers its slots');

  // sharing turned off: the same
  t.start('j1:5');
  t.share(false);
  assert.deepEqual(last(t.sent, 'eng.release'), { type: 'eng.release', partition: 'j1:5', attempt: 1, reason: 'sharing was turned off' });
  up = last(t.sent, 'update').engine;
  assert.equal(up.slots, 0);
  assert.equal(up.profile.blocked, 'sharing is off');
  t.share(true);
  assert.equal(last(t.sent, 'update').engine.slots, 2);

  // the coordinator cancels: the worker stops and nothing is sent for it
  t.start('j1:6');
  const w6 = t.workers.at(-1);
  t.E.handle({ type: 'eng.cancel', partition: 'j1:6', attempt: 1 });
  assert.equal(w6.terminated, true);
  assert.equal(t.E.running, 0);
  assert.equal(t.texts.length, 2);

  // a worker that dies is reported as the browser's failure, not the data's
  t.start('j1:7');
  t.workers.at(-1).onerror({ message: 'out of memory' });
  const crash = JSON.parse(t.texts.at(-1));
  assert.deepEqual([crash.type, crash.partition, crash.ok, crash.errorKind], ['eng.done', 'j1:7', false, 'infra']);
  assert.match(crash.error, /out of memory/);
  t.window.close();
}

{ // a low battery that isn't charging offers nothing until it charges
  const on = {};
  const battery = { charging: false, level: 0.15, addEventListener: (k, f) => { on[k] = f; } };
  const t = tab({ battery });
  await t.E.ready();
  let d = t.describe();
  assert.deepEqual(d.profile.battery, { charging: false, level: 0.15 });
  assert.equal(d.slots, 0);
  assert.equal(d.profile.blocked, 'the battery is low');
  battery.charging = true;
  on.chargingchange();
  d = last(t.sent, 'update').engine;
  assert.equal(d.slots, 2);
  assert.deepEqual(d.profile.battery, { charging: true, level: 0.15 });
  t.window.close();
}

{ // a browser without Web Workers says why and offers nothing
  const t = tab({ workers: false });
  const p = plain(await t.E.ready());
  assert.equal(p.ok, false);
  assert.equal(p.why, 'This browser lacks Web Workers.');
  assert.equal(t.describe().slots, 0);
  assert.equal(t.describe().profile.blocked, 'This browser lacks Web Workers.');
  t.window.close();
}

// ---------------------------------------------------------------- engine-worker.js, as a worker runs it
globalThis.self = globalThis;
globalThis.importScripts = (name) => (0, eval)(read(name));
const posted = [];
globalThis.postMessage = (m) => posted.push(m);
const O = (() => { (0, eval)(read('engine-worker.js')); return globalThis.YHEngineOps; })();
const worker = globalThis.onmessage;

let input = new Uint8Array();
let inputStatus = 200;
const puts = [];
globalThis.fetch = async (url, opts = {}) => {
  const method = opts.method || 'GET';
  assert.equal(opts.headers.Authorization, 'Bearer tok-1', 'every request carries the attempt token');
  if (method === 'GET' && url === '/api/engine/attempts/j1%3A0/1/input') {
    assert.equal(opts.cache, 'no-store');
    const body = new ReadableStream({ // in pieces that split records, as a network would
      start(c) { for (let i = 0; i < input.length; i += 777) c.enqueue(input.slice(i, i + 777)); c.close(); },
    });
    return { ok: inputStatus < 400, status: inputStatus, body };
  }
  if (method === 'PUT' && url === '/api/engine/attempts/j1%3A0/1/outputs/state.json') {
    puts.push(opts.body);
    return { ok: true, status: 200, json: async () => ({ sha: 'ab'.repeat(32), size: opts.body.length }) };
  }
  throw new Error(`unexpected ${method} ${url}`);
};

async function runTask(workload, lines, extra = {}) {
  input = new TextEncoder().encode(lines.join('\n') + '\n');
  const task = { workload, format: 'jsonl', header: null, input: { kind: 'range', start: 0, end: input.length }, runtime: { kind: 'builtin' },
    onBadRecord: 'fail', ...extra };
  posted.length = 0;
  await worker({ data: { taskJSON: JSON.stringify(task), partition: 'j1:0', attempt: 1, token: 'tok-1' } });
  const done = posted.filter((m) => m.type === 'done');
  assert.equal(done.length, 1);
  return { text: done[0].text, msg: JSON.parse(done[0].text) };
}
const expectedState = (workload) => {
  const op = O.makeOp(workload);
  O.runPartition(op, 'jsonl', null, [input], 0);
  return O.stateJSON(op.state());
};

{ // a small state comes back inline, as the exact text the operators wrote (5.0 keeps its decimal point)
  const wl = { type: 'batch.groupby', by: ['g'], where: [], metrics: [{ op: 'count' }, { op: 'sum', field: 'x' }, { op: 'max', field: 'x' }] };
  const lines = Array.from({ length: 3000 }, (_, i) => `{"g":"${'abc'[i % 3]}","x":${i % 2 ? '5.0' : i}}`);
  const { text, msg } = await runTask(wl, lines);
  assert.equal(msg.ok, true, text);
  assert.deepEqual([msg.type, msg.partition, msg.attempt, msg.records, msg.skipped, msg.bytes], ['eng.done', 'j1:0', 1, 3000, 0, input.length]);
  assert.deepEqual(msg.outputs, {});
  assert.equal(text.slice(text.indexOf('"state":') + 8, text.indexOf(',"outputs"')), expectedState(wl));
}

{ // a large state is uploaded as an output instead
  const wl = { type: 'batch.groupby', by: ['id'], where: [], metrics: [{ op: 'count' }, { op: 'sum', field: 'x' }] };
  const lines = Array.from({ length: 20000 }, (_, i) => `{"id":"item-${i}","x":${i}}`);
  const { msg } = await runTask(wl, lines);
  assert.equal(msg.ok, true);
  assert.ok(expectedState(wl).length > 256 * 1024, 'the test needs a state above the inline limit');
  assert.equal(puts.length, 1);
  assert.equal(puts[0], expectedState(wl));
  assert.equal(msg.stateOutput, 'state.json');
  assert.deepEqual(msg.outputs, { 'state.json': { sha: 'ab'.repeat(32), size: puts[0].length } });
  assert.equal(msg.state, undefined);
}

{ // what fails, and how the coordinator is told
  const agg = { type: 'batch.aggregate', where: [], metrics: [{ op: 'count' }] };
  let r = await runTask({ ...agg, where: [{ field: 'g', op: 'regex', value: '^a' }] }, ['{"g":"a"}']);
  assert.deepEqual([r.msg.ok, r.msg.errorKind], [false, 'unsupported'], 'a computer takes it instead');
  r = await runTask(agg, ['{"g":"a"}'], { runtime: { kind: 'managed-python' } });
  assert.deepEqual([r.msg.errorKind, r.msg.error], ['unsupported', 'your own code, which runs only on your computers']);
  r = await runTask(agg, ['{"g":"a"}', '{not json'], {});
  assert.equal(r.msg.errorKind, 'data', 'a bad record is the data’s fault');
  inputStatus = 403;
  r = await runTask(agg, ['{"g":"a"}']);
  assert.deepEqual([r.msg.errorKind, r.msg.error], ['infra', 'the coordinator answered 403 for the input']);
  inputStatus = 200;
}

{ // a stopped worker sends nothing more
  await worker({ data: { type: 'stop' } });
  posted.length = 0;
  input = new TextEncoder().encode('{"g":"a"}\n');
  await worker({ data: { taskJSON: JSON.stringify({ workload: { type: 'batch.aggregate', where: [], metrics: [{ op: 'count' }] }, format: 'jsonl', header: null,
    input: { kind: 'range', start: 0, end: input.length }, runtime: { kind: 'builtin' } }), partition: 'j1:0', attempt: 1, token: 'tok-1' } });
  assert.deepEqual(posted, []);
}

console.log('engine.js and engine-worker.js smoke test passed');
