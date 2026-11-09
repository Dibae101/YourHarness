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
const clf = { id: 'm1', ref: 'model:m1', name: 'clf.zip', adapter: 'python', format: 'zip', sha: 'ab'.repeat(32), size: 2000, files: ['model.py', 'weights.json'], createdAt: 1, createdBy: 'Mac' };
const scored = { id: 'j3', name: 'Score <b>it</b>', status: 'done', workload: 'ml.evaluate', total: 4, counts: { done: 4 }, perDevice: [{ name: 'Mac', n: 2 }, { name: 'PC', n: 2 }], createdAt: 4,
  startedAt: 4, finishedAt: 6, outputs: { records: 8, bytes: 400 }, validation: { level: 'spot-checked', checks: ['2 of 4 partitions were run again on Mac, PC and matched exactly.'] },
  result: { samples: 8, correct: 6, accuracy: 0.75, unlabeled: 0, macro: { precision: 0.75, recall: 0.75, f1: 0.75 }, weighted: { precision: 0.75, recall: 0.75, f1: 0.75 },
    classes: [{ label: 'a', support: 4, predicted: 4, correct: 3, precision: 0.75, recall: 0.75, f1: 0.75 }, { label: '<i>b</i>', support: 4, predicted: 4, correct: 3, precision: 0.75, recall: 0.75, f1: 0.75 }],
    confusion: { labels: ['a', '<i>b</i>'], rows: [[3, 1], [1, 3]] }, latency: { msPerRecord: 0.02, note: 'Measured on each computer.' } } };
const steps = (py) => [`uv venv --python ${py} <YourHarness folder>/envs/env-0123456789abcdef0123`, 'uv pip install scikit-learn==1.5.2', 'a check that the packages import and the job’s worker starts'];
const waiting = { id: 'j2', name: 'Fit', status: 'needs_approval', workload: 'python.map', total: 4, counts: { ready: 4 }, perDevice: [], createdAt: 3,
  runtime: { kind: 'managed-python', python: '3.11', requirements: ['scikit-learn==1.5.2'] }, setup: [],
  setupPlan: [{ id: 'a', name: 'Mac', steps: steps('3.11'), downloads: 'If missing: uv 0.13.7 from GitHub (checksum checked), Python 3.11, and the packages from PyPI.' },
    { id: 'c', name: '<i>PC</i>', steps: steps('3.11'), downloads: 'If missing: uv 0.13.7 from GitHub (checksum checked), Python 3.11, and the packages from PyPI.' }] };
window.fetch = async (url, opts = {}) => {
  const method = opts.method || 'GET';
  const body = opts.body && typeof opts.body === 'string' ? JSON.parse(opts.body) : undefined;
  calls.push({ method, url, body });
  const json = (o, status = 200) => ({ ok: status < 400, status, headers: { get: () => 'application/json' }, json: async () => o, text: async () => JSON.stringify(o) });
  if (url === '/api/engine') return json({ jobs: [], datasets: [ds], models: [clf], storage: { artifacts: 3, bytes: 200000 }, devices: [
    { id: 'a', name: 'Mac', kind: 'agent', owned: true, engine: { slots: 3, ops: ['batch.aggregate', 'python.map'], formats: ['jsonl', 'csv', 'text'], runtimes: ['builtin', 'native-python', 'managed-python'], busy: 0,
      envs: 2, accelerators: [{ kind: 'metal', name: 'Apple silicon GPU', memoryGB: null, driver: 'Metal' }] } },
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
  if (url === '/api/engine/jobs/j2/approve-setup') return json({ job: { ...waiting, status: 'running', setupPlan: null, note: 'Preparing its Python environment on Mac.',
    setup: [{ device: 'Mac', phase: 'packages', note: 'Installing 1 package', error: null }] } });
  if (url.startsWith('/api/engine/models?') && method === 'PUT') return json({ model: { ...clf, id: 'm2', ref: 'model:m2', name: new URL(url, 'http://x').searchParams.get('name'), format: 'py', files: null } }, 201);
  if (url === '/api/engine/models/m2' && method === 'DELETE') return json({ ok: true });
  if (url === '/api/engine/jobs/j3') return json({ job: { ...scored, plan: { reasons: [] }, partitions: [], events: [], spec: {},
    provenance: { dataset: { ref: 'dataset:d1a2b3c4', name: 'sales.jsonl', sha256: 'cd'.repeat(32) }, model: { ref: 'model:m1', name: 'clf.zip', sha256: clf.sha }, environment: { kind: 'native-python' } } } });
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
assert.equal(py.runtime, undefined, 'the default is the Python YourHarness runs on');

// an environment of its own: a Python version and pinned packages
document.querySelector('[data-tab="new"]').click();
assert.equal(document.querySelector('#bj-reqs').closest('[data-runtime]').hidden, true);
set('#bj-runtime', 'managed-python');
assert.equal(document.querySelector('#bj-reqs').closest('[data-runtime]').hidden, false);
document.querySelector('#bj-pyver').value = '3.11';
document.querySelector('#bj-reqs').value = 'scikit-learn==1.5.2\n# a comment\n\n numpy \n';
document.querySelector('#bj-form').dispatchEvent(new window.Event('submit', { bubbles: true, cancelable: true }));
await settle();
const mp = calls.filter((c) => c.method === 'POST' && c.url === '/api/engine/jobs').pop().body.job.spec;
assert.deepEqual(mp.runtime, { kind: 'managed-python', python: '3.11', requirements: ['scikit-learn==1.5.2', 'numpy'] });
const before = calls.length;
document.querySelector('[data-tab="new"]').click();
document.querySelector('#bj-reqs').value = '--index-url https://example.invalid/simple';
document.querySelector('#bj-form').dispatchEvent(new window.Event('submit', { bubbles: true, cancelable: true }));
await settle();
assert.match(document.querySelector('#bj-status').textContent, /isn't a package requirement/);
assert.equal(calls.length, before, 'a pip option is refused before anything is sent');
document.querySelector('#bj-reqs').value = 'scikit-learn==1.5.2';
set('#bj-runtime', 'native-python');

// the job waits for your approval; the card says what each computer would run
document.querySelector('[data-tab="jobs"]').click();
msg({ type: 'eng.job', job: waiting });
let setup = document.querySelector('[data-setup="j2"]');
assert.ok(setup, 'a waiting job shows its setup');
assert.match(document.querySelector('[data-job="j2"]').textContent, /Needs your approval/);
assert.match(setup.textContent, /uv pip install scikit-learn==1\.5\.2/);
assert.match(setup.textContent, /Python 3\.11/);
assert.ok(!setup.querySelector('i'), 'computer names are escaped');
setup.querySelector('[data-setup-device="c"]').checked = false;
setup.querySelector('[data-setup-downloads]').checked = true;
msg({ type: 'eng.job', job: { ...waiting } });  // a live update keeps what you ticked
setup = document.querySelector('[data-setup="j2"]');
assert.equal(setup.querySelector('[data-setup-device="c"]').checked, false);
assert.equal(setup.querySelector('[data-setup-downloads]').checked, true);
setup.querySelector('[data-act="approve-setup"]').click();
await settle();
assert.deepEqual(calls.find((c) => c.url === '/api/engine/jobs/j2/approve-setup').body, { devices: ['a'], downloads: true });
assert.ok(!document.querySelector('[data-setup="j2"]'), 'the approval card goes away');
assert.match(document.querySelector('[data-job="j2"]').textContent, /Python environment: Mac Installing packages/);

// a model scored on labeled records
document.querySelector('[data-tab="new"]').click();
set('#bj-input', 'dataset:d1a2b3c4');
set('#bj-kind', 'ml.evaluate');
assert.equal(document.querySelector('#bj-model').value, 'model:m1');
assert.equal(document.querySelector('#bj-trust').disabled, true, 'a model is your own code');
assert.equal(document.querySelector('#bj-runtime').closest('section').hidden, false, 'it can run in its own environment too');
document.querySelector('#bj-features').value = 'x, y';
document.querySelector('#bj-id').value = 'id';
document.querySelector('#bj-preds').checked = true;
document.querySelector('#bj-labels').value = 'a, b';
document.querySelector('#bj-form').dispatchEvent(new window.Event('submit', { bubbles: true, cancelable: true }));
await settle();
const ev = calls.filter((c) => c.method === 'POST' && c.url === '/api/engine/jobs').pop().body.job.spec;
assert.deepEqual(ev.workload, { type: 'ml.evaluate', label: 'label', features: ['x', 'y'], id: 'id', batch: 64, predictions: true, labels: ['a', 'b'] });
assert.deepEqual(ev.inputs, { dataset: { ref: 'dataset:d1a2b3c4' }, model: { ref: 'model:m1' } });
assert.equal(ev.security.allowedTrust, 'owned-only');
// checking it on another computer, then the scores
msg({ type: 'eng.job', job: { ...scored, status: 'verifying', result: null, outputs: null, validation: null, verify: { sample: 2, matched: 1, running: ['PC'] } } });
assert.match(document.querySelector('[data-job="j3"]').textContent, /Checking/);
assert.match(document.querySelector('[data-job="j3"]').textContent, /1 of 2 partitions run again and matched · running on PC/);
msg({ type: 'eng.job', job: scored });
const sc = document.querySelector('[data-job="j3"]');
assert.match(sc.textContent, /Accuracy 75\.0% \(6 of 8\)/);
assert.match(sc.textContent, /8 predictions written/);
assert.ok(!sc.querySelector('h2 b'), 'job names are escaped');
document.querySelector('[data-act="detail"][data-id="j3"]').click();
await settle();
const det = document.querySelector('#bj-detail');
assert.match(det.textContent, /Confusion matrix/);
assert.equal(det.querySelectorAll('.bj-diag').length, 2);
assert.ok(!det.querySelector('td i, th i'), 'labels are escaped');
assert.match(det.textContent, /model clf\.zip abababababab/);
set('#bj-kind', 'batch.aggregate');

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

// models: listed with their digest, uploaded, deleted
assert.match(document.querySelector('#bj-models').textContent, new RegExp(clf.sha));
const mf = document.querySelector('#bj-model-file');
Object.defineProperty(mf, 'files', { value: [new window.File(['def predict(model, batch):\n    return batch\n'], 'rule.py')] });
document.querySelector('[data-act="add-model"]').click();
await settle();
const up = calls.find((c) => c.method === 'PUT' && c.url.startsWith('/api/engine/models?'));
assert.equal(new URL(up.url, 'http://x').searchParams.get('name'), 'rule.py');
assert.match(document.querySelector('#bj-model-status').textContent, /Added rule\.py/);
assert.equal(document.querySelectorAll('#bj-model option').length, 2, 'the new model is offered for jobs');
document.querySelector('[data-act="delete-model"][data-id="m2"]').click();
await settle();
assert.ok(calls.find((c) => c.method === 'DELETE' && c.url === '/api/engine/models/m2'));
assert.equal(document.querySelectorAll('#bj-model option').length, 1);

// computers
document.querySelector('[data-tab="computers"]').click();
const comp = document.querySelector('#bj-comp').textContent;
assert.match(comp, /Mac/); assert.match(comp, /Browser tabs don’t run batch jobs yet/);
assert.match(comp, /GPU: Apple silicon GPU/); assert.match(comp, /2 prepared environments/);
console.log('batch.js smoke test passed:', calls.length, 'requests');
