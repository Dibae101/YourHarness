// Headless smoke test of the Home screen (yourharness/web/home.js) against a stubbed coordinator.
// Needs jsdom:  npm install --no-save jsdom@26   then:  node tests/web/home_smoke.mjs
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import assert from 'node:assert/strict';

const src = fs.readFileSync(process.argv[2] || new URL('../../yourharness/web/home.js', import.meta.url), 'utf8');
const dom = new JSDOM('<!doctype html><body><section class="win" id="win-home"><span id="wsub-home"></span><div class="win-body"><div id="home"></div></div></section></body>',
  { url: 'http://127.0.0.1:8420/', runScripts: 'outside-only', pretendToBeVisual: true });
const { window } = dom;
const { document } = window;
const now = Date.now() / 1000;
const lim = (o) => ({ paused: false, wantSharing: true, cap: null, mobile: false, ...o });
const DEVICES = [
  { id: 'pc', name: 'DESKTOP <b>PC</b>', kind: 'agent', owned: true, threads: 16, sharing: true, busy: 0, reserved: {}, info: { platform: 'Linux (WSL)', cores: 16, memoryGB: 11.3, memoryFreeGB: 9.2, diskFreeGB: 954 },
    limits: lim({ threads: 16, wantThreads: 16 }) },
  { id: 'mac', name: 'batman', kind: 'agent', owned: true, coordinator: true, threads: 7, sharing: true, busy: 0, reserved: { engine: 2 }, info: { platform: 'macOS', cores: 8, memoryGB: 8, memoryFreeGB: 1.5 },
    limits: lim({ threads: 7, wantThreads: 7 }) },
  { id: 'phone', name: 'iPhone', kind: 'browser', owned: true, threads: 1, sharing: true, busy: 0, info: { platform: 'iPhone', cores: 6 }, limits: lim({ threads: 1, wantThreads: 4, mobile: true }) },
  { id: 'pad', name: 'iPad', kind: 'browser', owned: true, threads: 3, sharing: false, busy: 0, info: { platform: 'iPad', cores: 8 }, limits: lim({ paused: true, threads: 3, wantThreads: 3, cap: 3, mobile: true }) },
  { id: 'me1', name: 'Mac tab', kind: 'browser', owned: true, threads: 4, sharing: true, busy: 0, stale: true, info: { platform: 'macOS', cores: 8 }, limits: lim({ threads: 4, wantThreads: 4 }) },
  { id: 'friend', name: 'Sam’s laptop', kind: 'agent', owned: false, threads: 2, sharing: true, busy: 1, info: { platform: 'Windows', cores: 4 }, limits: lim({ threads: 2, wantThreads: 4, cap: 2 }) },
];
const ENGINE = [
  { id: 'e1', name: 'Totals <b>x</b>', status: 'running', workload: 'batch.aggregate', total: 10, counts: { done: 4 }, perDevice: [{ name: 'PC', n: 3 }, { name: 'batman', n: 1 }], createdAt: now - 70, startedAt: now - 65 },
  { id: 'e2', name: 'tests: proj', status: 'done', workload: 'tests.pytest', total: 16, counts: { done: 16 }, perDevice: [], createdAt: now - 900, startedAt: now - 890, finishedAt: now - 600 },
  { id: 'e3', name: 'Last week', status: 'done', workload: 'batch.groupby', total: 2, counts: { done: 2 }, perDevice: [], createdAt: now - 9e5, finishedAt: now - 9e5 },
];
const RUNS = [{ id: 'r1', title: 'Fix <i>bug</i>', status: 'running', createdAt: now - 30, updatedAt: now,
  tasks: [{ id: 't1', runId: 'r1', status: 'running', deviceName: 'DESKTOP <b>PC</b>', phase: 'agent' }, { id: 't2', runId: 'r1', status: 'done', deviceName: 'batman' }] }];
const calls = [];
window.fetch = async (url, opts = {}) => {
  const method = opts.method || 'GET';
  const body = opts.body ? JSON.parse(opts.body) : undefined;
  calls.push({ method, url, body });
  const json = (o, status = 200) => ({ ok: status < 400, status, json: async () => o });
  if (url === '/api/engine') return json({ jobs: ENGINE });
  if (url === '/api/agents') return json({ runs: RUNS });
  if (url === '/api/jobs') return json({ jobs: [] });
  if (url === '/api/me') return json({ deviceId: 'me1', caps: ['admin', 'submit'] });
  if (url === '/api/devices') return json({ devices: DEVICES });
  const m = url.match(/^\/api\/devices\/([^/]+)\/limits$/);
  if (m && method === 'POST') {
    if (m[1] === 'friend') return json({ error: 'The pool answered no.' }, 403);
    const d = DEVICES.find((x) => x.id === m[1]);
    const l = { ...d.limits, ...('paused' in body ? { paused: body.paused } : {}), ...('threads' in body ? { cap: body.threads } : {}) };
    l.threads = Math.min(l.wantThreads, l.cap ?? (l.mobile ? 1 : l.wantThreads));
    return json({ limits: {}, device: { ...d, limits: l, threads: l.threads, sharing: l.wantSharing && !l.paused } });
  }
  throw new Error(`unexpected ${method} ${url}`);
};
window.eval(src);
const settle = () => new Promise((r) => setTimeout(r, 60));
const msg = (m) => window.dispatchEvent(new window.CustomEvent('yh:msg', { detail: m }));
const $ = (s) => document.querySelector(s);
const $$ = (s) => [...document.querySelectorAll(s)];
const card = (id) => $(`.hm-dev[data-id="${id}"]`);
const lastCall = () => calls[calls.length - 1];

// opened before the welcome message arrived: it asks for who it is and the devices itself
window.YHHome.opened();
await settle(); await settle();
assert.ok(calls.some((c) => c.url === '/api/me') && calls.some((c) => c.url === '/api/devices'));
const home = $('#home');
assert.deepEqual($$('.hm-stat').map((s) => s.textContent), ['6 devices connected', '26 threads shared', '2 working', '2 jobs running']);
assert.equal($('#wsub-home').textContent, '2 running');

// running now: the batch job and the agent run, newest first, names as text
const live = $$('.hm-job');
assert.equal(live.length, 2);
assert.equal(live[0].querySelector('.hm-job-name').textContent, 'Fix <i>bug</i>');
assert.match(live[0].querySelector('.hm-job-state').textContent, /Running · 1 of 2 tasks done/);
assert.equal(live[1].querySelector('.hm-job-name').textContent, 'Totals <b>x</b>');
assert.equal(live[1].querySelector('.hm-bar').getAttribute('aria-valuenow'), '40');
assert.match(live[1].querySelector('.hm-job-state').textContent, /Running · 4 of 10 pieces · 1 min 5 s/);
assert.deepEqual($$('.hm-job')[1].querySelectorAll('.hm-chip b').length, 2);
assert.equal(live[1].querySelector('[data-open]').dataset.open, 'batch');
assert.equal(live[0].querySelector('[data-open]').dataset.open, 'agents');
assert.equal(home.querySelectorAll('b i, b b, .hm-chip b b').length, 0, 'nothing from the pool is read as HTML');
// finished recently: the last day only
const recent = $$('.hm-recent-row');
assert.equal(recent.length, 1);
assert.match(recent[0].textContent, /Test run\s+tests: proj/);
assert.match(recent[0].textContent, /Done 10 min ago/);
assert.equal(recent[0].querySelector('.hm-mark').dataset.ok, 'true');

// devices: computers first (the one running the pool first), then tabs, phones and tablets
const groups = $$('.hm-sub-h').map((h) => h.textContent);
assert.deepEqual(groups, ['Finished recently', 'Computers', 'Phones, tablets and browser tabs']);
const lists = $$('.hm-sec')[1].querySelectorAll('.hm-list');
assert.deepEqual([...lists[0].children].map((li) => li.dataset.id), ['mac', 'pc', 'friend']);
assert.deepEqual([...lists[1].children].map((li) => li.dataset.id), ['me1', 'pad', 'phone']);
assert.equal(card('pc').querySelector('.hm-dev-name b').textContent, 'DESKTOP <b>PC</b>');
assert.equal(card('mac').querySelector('.hm-dev-sub').textContent, 'macOS · 8 cores · 1.5 of 8 GB memory free · runs the pool');
assert.equal(card('phone').querySelector('.hm-dev-sub').textContent, 'iPhone · 6 cores');
assert.equal(card('me1').querySelector('.hm-dev-sub').textContent, 'Browser tab, macOS · 8 cores');
assert.equal(card('mac').dataset.state, 'working');
assert.equal(card('pc').dataset.state, 'ready');
assert.equal(card('phone').dataset.state, 'ready');
assert.equal(card('phone').querySelector('.hm-dev-state').textContent, 'Ready for small tasks');
assert.equal(card('pad').dataset.state, 'paused');
assert.equal(card('me1').dataset.state, 'stale');
assert.match(card('me1').querySelector('.hm-dev-name').textContent, /This device/);
assert.match(card('friend').querySelector('.hm-dev-name').textContent, /A friend’s/);
assert.equal(card('friend').dataset.state, 'working');

// the switch and the threads: a phone starts at 1 of the 4 it offers
const step = (id, dir) => card(id).querySelector(`button[data-step="${dir}"]`);
assert.equal(card('phone').querySelector('.hm-step-n').textContent, '1');
assert.match(card('phone').querySelector('.hm-step-of').textContent, /of 4 threads/);
assert.ok(step('phone', -1).disabled && !step('phone', 1).disabled);
step('phone', 1).click();
await settle();
assert.deepEqual(lastCall(), { method: 'POST', url: '/api/devices/phone/limits', body: { threads: 2 } });
assert.equal(card('phone').querySelector('.hm-step-n').textContent, '2');
// a computer at every thread it offers: one fewer is a limit, back up to all is no limit
assert.ok(step('pc', 1).disabled && !step('pc', -1).disabled);
step('pc', -1).click();
await settle();
assert.deepEqual(lastCall().body, { threads: 15 });
assert.equal(card('pc').querySelector('.hm-step-n').textContent, '15');
step('pc', 1).click();
await settle();
assert.deepEqual(lastCall().body, { threads: null });
assert.equal(card('pc').querySelector('.hm-step-n').textContent, '16');
// the paused tablet: its switch is off and its threads can't change until it's back on
const sw = (id) => card(id).querySelector('input[data-share]');
assert.equal(sw('pad').checked, false);
assert.ok(step('pad', 1).disabled && step('pad', -1).disabled);
sw('pad').checked = true;
sw('pad').dispatchEvent(new window.Event('change', { bubbles: true }));
await settle();
assert.deepEqual(lastCall(), { method: 'POST', url: '/api/devices/pad/limits', body: { paused: false } });
assert.equal(card('pad').dataset.state, 'ready');
sw('mac').checked = false;
sw('mac').dispatchEvent(new window.Event('change', { bubbles: true }));
await settle();
assert.deepEqual(lastCall().body, { paused: true });
assert.equal(card('mac').dataset.state, 'paused');
assert.match(card('mac').querySelector('.hm-dev-state').textContent, /Paused: takes no new work/);
// refused: the change is undone and the reason shown
sw('friend').checked = false;
sw('friend').dispatchEvent(new window.Event('change', { bubbles: true }));
await settle();
assert.equal(sw('friend').checked, true);
assert.match($('.hm-status').textContent, /Sam’s laptop: The pool answered no\./);

// live: the welcome message, then work finishing and starting
msg({ type: 'welcome', id: 'me1', caps: ['admin', 'submit'], devices: DEVICES, jobs: [] });
msg({ type: 'eng.job', job: { id: 'e1', status: 'done', finishedAt: now, counts: { done: 10 } } });
msg({ type: 'agents.task', task: { id: 't1', runId: 'r1', status: 'done', deviceName: 'DESKTOP <b>PC</b>' } });
msg({ type: 'job', job: { id: 'o1', kind: 'primes', title: 'Primes below 3e9', status: 'running', total: 300, done: 0, createdAt: Date.now(), startedAt: Date.now(), perDevice: {} } });
msg({ type: 'progress', jobId: 'o1', done: 30, deviceId: 'pc', deviceName: 'DESKTOP <b>PC</b>' });
await settle();
const names = $$('.hm-job .hm-job-name').map((b) => b.textContent);
assert.deepEqual(names, ['Primes below 3e9', 'Fix <i>bug</i>']);
const prim = $$('.hm-job')[0];
assert.match(prim.querySelector('.hm-kind').textContent, /Compute job/);
assert.match(prim.querySelector('.hm-job-state').textContent, /Running · 30 of 300 pieces · 0 s/);
assert.match(prim.querySelector('.hm-who').textContent, /Pieces done by\s*DESKTOP <b>PC<\/b> 1/);
assert.match($$('.hm-job')[1].querySelector('.hm-job-state').textContent, /2 of 2 tasks done/);
assert.equal($$('.hm-recent-row').length, 2);
assert.match($$('.hm-recent-row')[0].textContent, /Totals <b>x<\/b>/);

// a finger held on a control: the list isn't swapped out under it until it lifts
const before = card('phone');
before.querySelector('button[data-step="1"]').dispatchEvent(new window.Event('pointerdown', { bubbles: true }));
msg({ type: 'devices', devices: DEVICES.filter((d) => d.id !== 'friend') });
await settle();
assert.equal(card('phone'), before, 'not re-rendered while held');
assert.ok(card('friend'));
window.dispatchEvent(new window.Event('pointerup'));
await settle();
assert.equal(card('friend'), null);
assert.notEqual(card('phone'), before);

// someone who isn't the pool's owner sees what each device shares, without the controls
msg({ type: 'welcome', id: 'me1', caps: ['submit'], devices: DEVICES, jobs: [] });
await settle();
assert.equal($$('.hm-ctrl').length, 0);
assert.equal($$('.hm-help').length, 0);
assert.equal(card('pc').querySelector('.hm-share').textContent, 'Shares 16 of 16 threads');
assert.equal(card('mac').querySelector('.hm-share').textContent, 'Paused', 'paused above');

// nothing running
msg({ type: 'eng.job', job: { id: 'e1', deleted: true } });
msg({ type: 'agents.run', run: { ...RUNS[0], status: 'done', updatedAt: now } });
msg({ type: 'job', job: { id: 'o1', kind: 'primes', title: 'Primes below 3e9', status: 'done', total: 300, done: 300, createdAt: Date.now(), finishedAt: Date.now(), perDevice: {} } });
await settle();
assert.match($('.hm-empty').textContent, /Nothing is running/);
assert.equal($('#wsub-home').textContent, '');
assert.equal($$('.hm-recent-row').length, 3);

window.close();
console.log('home smoke: ok');
