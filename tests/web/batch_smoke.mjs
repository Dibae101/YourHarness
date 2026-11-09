// Headless smoke test of the Batch app (yourharness/web/batch.js) against a stubbed coordinator.
// Needs jsdom:  npm install --no-save jsdom@26   then:  node tests/web/batch_smoke.mjs
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import assert from 'node:assert/strict';

const src = fs.readFileSync(process.argv[2] || new URL('../../yourharness/web/batch.js', import.meta.url), 'utf8');
const dom = new JSDOM('<!doctype html><body><div id="bj-body"></div></body>', { url: 'http://127.0.0.1:8420/', runScripts: 'outside-only' });
const { window } = dom;
const { document } = window;
window.CSS = { escape: (s) => String(s).replace(/[^\w-]/g, '\\$&') };
window.alert = (m) => { throw new Error(`alert: ${m}`); };
window.confirm = () => true;
window.HTMLElement.prototype.scrollIntoView = () => {};
const calls = [];
const ds = { id: 'd1a2b3c4', ref: 'dataset:d1a2b3c4', name: 'sales.jsonl', format: 'jsonl', size: 123456, lines: 900, header: null, classification: 'owned-private', createdAt: 1, createdBy: 'Mac' };
window.fetch = async (url, opts = {}) => {
  const method = opts.method || 'GET';
  const body = opts.body && typeof opts.body === 'string' ? JSON.parse(opts.body) : undefined;
  calls.push({ method, url, body });
  const json = (o, status = 200) => ({ ok: status < 400, status, headers: { get: () => 'application/json' }, json: async () => o, text: async () => JSON.stringify(o) });
  if (url === '/api/engine') return json({ jobs: [], datasets: [ds], storage: { artifacts: 3, bytes: 200000 }, devices: [
    { id: 'a', name: 'Mac', kind: 'agent', owned: true, engine: { slots: 3, ops: ['batch.aggregate', 'python.map'], formats: ['jsonl', 'csv', 'text'], runtimes: ['builtin', 'native-python'], busy: 0 } },
    { id: 'b', name: 'iPhone', kind: 'browser', owned: true, engine: null }] });
  if (url === '/api/engine/plan') return json({ plan: { decision: 'RUN_DISTRIBUTED', reasons: ['About 3 s spread over 2 computers.'], unit: 'bytes',
    devices: [{ name: 'Mac', owned: true, eligible: true, rate: 25e6, measured: false }, { name: '<b>PC</b>', owned: false, eligible: false, why: 'is a friend’s computer' }] } });
  if (url === '/api/engine/jobs' && method === 'POST') return json({ job: { id: 'j1', name: body.job.metadata.name, status: 'running', workload: body.job.spec.workload.type, total: 4, counts: { ready: 4 }, perDevice: [], createdAt: 2, dataset: { name: 'sales.jsonl', size: 123456 } } }, 201);
  if (url === '/api/files') return json({ files: [{ name: 'big.csv', size: 5000 }] });
  if (url === '/api/pool') return json({ files: [] });
  if (url.startsWith('/api/engine/datasets/import')) return json({ dataset: { ...ds, id: 'd9999999', ref: 'dataset:d9999999', name: 'big.csv', format: 'csv' } }, 201);
  if (url === '/api/engine/datasets/d1a2b3c4/preview') return json({ records: [{ amount: 1 }, { amount: 2 }] });
  if (url === '/api/engine/datasets/d1a2b3c4' && method === 'POST') return json({ dataset: { ...ds, classification: body.classification } });
  if (url === '/api/engine/jobs/j1') return json({ job: { id: 'j1', name: 'Totals', plan: { reasons: ['Spread.'] }, result: {}, partitions: [{ idx: 0, status: 'done', device: 'Mac', bytes: 1000, records: 10, seconds: 0.2, attempt: 1, moves: 0 }], events: [], spec: {} } });
  throw new Error(`unexpected ${method} ${url}`);
};
window.eval(src);
const tick = () => new Promise((r) => setTimeout(r, 0));
const settle = async () => { for (let i = 0; i < 5; i++) await tick(); };
const msg = (m) => window.dispatchEvent(new window.CustomEvent('yh:msg', { detail: m }));
const set = (sel, v) => { const el = document.querySelector(sel); el.value = v; el.dispatchEvent(new window.Event('change', { bubbles: true })); };

window.YHBatch.opened();
await settle();
assert.match(document.querySelector('#bj-jobs').textContent, /No batch jobs yet/);

// a Totals job with two totals and a condition
document.querySelector('[data-tab="new"]').click();
assert.equal(document.querySelector('#bj-input').value, 'dataset:d1a2b3c4');
document.querySelector('#bj-name').value = 'Totals';
document.querySelector('[data-act="add-metric"]').click();
const rows = document.querySelectorAll('#bj-metrics .bj-row');
rows[1].querySelector('[data-m="op"]').value = 'mean';
rows[1].querySelector('[data-m="field"]').value = 'amount';
document.querySelector('[data-act="add-where"]').click();
const w = document.querySelector('#bj-where .bj-row');
w.querySelector('[data-w="field"]').value = 'qty';
w.querySelector('[data-w="op"]').value = '>=';
w.querySelector('[data-w="value"]').value = '3';
set('#bj-mode', 'distributed');
document.querySelector('[data-act="plan"]').click();
await settle();
const planBox = document.querySelector('#bj-plan');
assert.equal(planBox.hidden, false);
assert.match(planBox.textContent, /Spread over computers/);
assert.ok(!planBox.querySelector('b b'), 'device names are escaped');
assert.match(planBox.textContent, /friend’s computer/);
document.querySelector('#bj-form').dispatchEvent(new window.Event('submit', { bubbles: true, cancelable: true }));
await settle();
const post = calls.find((c) => c.method === 'POST' && c.url === '/api/engine/jobs');
const job = post.body.job;
assert.equal(job.apiVersion, 'yourharness.dev/v1alpha1');
assert.equal(job.metadata.name, 'Totals');
assert.deepEqual(job.spec.workload, { type: 'batch.aggregate', metrics: [{ op: 'count' }, { op: 'mean', field: 'amount' }], where: [{ field: 'qty', op: '>=', value: 3 }] });
assert.deepEqual(job.spec.inputs, { dataset: { ref: 'dataset:d1a2b3c4' } });
assert.equal(job.spec.mode, 'distributed');
assert.equal(job.spec.output.spotCheck, 2);
assert.equal(document.querySelector('[data-pane="jobs"]').hidden, false, 'goes to Jobs after starting');

// live progress, then the result
msg({ type: 'eng.job', job: { id: 'j1', name: '<img src=x>', status: 'running', workload: 'batch.aggregate', total: 4, counts: { done: 2, leased: 2 }, perDevice: [{ name: 'Mac', n: 2 }], createdAt: 2 } });
assert.ok(!document.querySelector('#bj-jobs img'), 'job names are escaped');
assert.equal(document.querySelector('#bj-jobs .bj-bar span').style.width, '50%');
msg({ type: 'eng.job', job: { id: 'j1', name: 'Totals', status: 'done', workload: 'batch.aggregate', total: 4, counts: { done: 4 }, perDevice: [{ name: 'Mac', n: 4 }],
  createdAt: 2, startedAt: 2, finishedAt: 5, result: { values: { count: 900, mean_amount: 12.5 } }, validation: { level: 'spot-checked', checks: ['2 of 4 partitions were computed again on the coordinator and matched exactly.'] } } });
const card = document.querySelector('#bj-jobs').textContent;
assert.match(card, /count/); assert.match(card, /900/); assert.match(card, /Spot-checked/);
assert.ok(document.querySelector('#bj-jobs a[download]').getAttribute('href').endsWith('/api/engine/jobs/j1/result'));
document.querySelector('[data-act="detail"][data-id="j1"]').click();
await settle();
assert.match(document.querySelector('#bj-detail').textContent, /Mac/);

// your own function over a parameter grid
document.querySelector('[data-tab="new"]').click();
set('#bj-kind', 'python.map');
set('#bj-input', '__items__');
assert.equal(document.querySelector('#bj-items-field').hidden, false);
assert.equal(document.querySelector('#bj-trust').disabled, true, 'your own code runs on your computers only');
document.querySelector('#bj-items').value = '{"lr": [0.1, 0.01], "depth": [2, 4]}';
set('#bj-reducer', 'best');
document.querySelector('#bj-best-key').value = 'score';
document.querySelector('#bj-form').dispatchEvent(new window.Event('submit', { bubbles: true, cancelable: true }));
await settle();
const py = calls.filter((c) => c.method === 'POST' && c.url === '/api/engine/jobs').pop().body.job.spec;
assert.deepEqual(py.inputs, { grid: { lr: [0.1, 0.01], depth: [2, 4] } });
assert.equal(py.workload.reducer, 'best');
assert.deepEqual(py.workload.bestBy, { key: 'score', order: 'max' });
assert.equal(py.security.allowedTrust, 'owned-only');

// datasets: import from the shared folder, preview, share
document.querySelector('[data-tab="datasets"]').click();
await settle();
set('#bj-import', 'shared:big.csv');
document.querySelector('[data-act="add-dataset"]').click();
await settle();
const imp = calls.find((c) => c.url === '/api/engine/datasets/import');
assert.deepEqual(imp.body, { source: 'shared', path: 'big.csv', classification: 'owned-private' });
assert.match(document.querySelector('#bj-datasets').textContent, /big\.csv/);
document.querySelector('[data-act="preview"][data-id="d1a2b3c4"]').click();
await settle();
assert.match(document.querySelector('[data-ds="d1a2b3c4"] pre').textContent, /"amount":1/);
document.querySelector('[data-act="share"][data-id="d1a2b3c4"]').click();
await settle();
assert.equal(calls.find((c) => c.url === '/api/engine/datasets/d1a2b3c4' && c.method === 'POST').body.classification, 'shareable');

// computers
document.querySelector('[data-tab="computers"]').click();
const comp = document.querySelector('#bj-comp').textContent;
assert.match(comp, /Mac/); assert.match(comp, /Browser tabs don’t run batch jobs yet/);
console.log('batch.js smoke test passed:', calls.length, 'requests');
