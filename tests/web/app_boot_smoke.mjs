// Headless boot of the whole page (index.html, app.js and home.js) against a stubbed coordinator:
// it opens on Home, the dock and the app tiles reach Home, and what the pool's owner allows this
// device shows under "This device".
// Needs jsdom:  npm install --no-save jsdom@26   then:  node tests/web/app_boot_smoke.mjs
import { JSDOM, VirtualConsole } from 'jsdom';
import fs from 'node:fs';
import assert from 'node:assert/strict';

const web = new URL('../../yourharness/web/', import.meta.url);
const html = fs.readFileSync(new URL('index.html', web), 'utf8').replace(/<script\b[^>]*><\/script>/g, ''); // run below, in order
const errors = [];
const vc = new VirtualConsole();
vc.on('jsdomError', (e) => { if (!/Not implemented/.test(e.message)) errors.push(e); });
const dom = new JSDOM(html, { url: 'http://127.0.0.1:8420/', runScripts: 'outside-only', pretendToBeVisual: true, virtualConsole: vc });
const { window } = dom;
const { document } = window;
window.matchMedia = (q) => ({ matches: false, media: q, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} });
const sockets = [];
window.WebSocket = class {
  constructor(url) {
    this.url = url; this.readyState = 0; this.sent = [];
    sockets.push(this);
    setTimeout(() => { this.readyState = 1; if (this.onopen) this.onopen(); }, 0);
  }
  send(text) { this.sent.push(JSON.parse(text)); }
  close() { this.readyState = 3; }
};
window.Worker = class { constructor() { this.onmessage = null; } postMessage() {} terminate() {} addEventListener() {} removeEventListener() {} };
// jsdom draws nothing: a 2D context whose every method and property is a harmless no-op
const noop = new Proxy(function () {}, { get: (t, k) => (k === Symbol.toPrimitive ? () => 0 : k === 'width' ? 0 : noop), apply: () => noop, set: () => true });
window.HTMLCanvasElement.prototype.getContext = () => noop;
const calls = [];
window.fetch = async (url, opts = {}) => {
  calls.push(`${opts.method || 'GET'} ${url}`);
  const json = (o) => ({ ok: true, status: 200, headers: { get: () => 'application/json' }, json: async () => o, text: async () => JSON.stringify(o) });
  if (url === '/api/engine') return json({ jobs: [], datasets: [], models: [], devices: [] });
  if (url === '/api/agents') return json({ runs: [] });
  if (url === '/api/jobs') return json({ jobs: [] });
  return json({});
};
for (const f of ['app.js', 'home.js']) window.eval(fs.readFileSync(new URL(f, web), 'utf8'));

const settle = () => new Promise((r) => setTimeout(r, 60));
const $ = (s) => document.querySelector(s);
const device = (o) => ({ threads: 1, sharing: true, runtimes: ['js'], workspace: false, busy: 0, done: 0, busyMs: 0, joinedAt: Date.now(), stale: false, host: null,
  coordinator: false, files: false, exec: null, load: null, owned: true, agentTasks: null, engine: null, reserved: {}, ...o });
const phoneLimits = { paused: false, threads: 1, wantThreads: 4, wantSharing: true, cap: null, mobile: true };
const devices = [
  device({ id: 'me1', name: 'Test phone', kind: 'browser', color: '#2563C9', info: { platform: 'iPhone', cores: 4 }, limits: phoneLimits }),
  device({ id: 'mac', name: 'Desk Mac', kind: 'agent', color: '#E0A100', threads: 6, coordinator: true, runtimes: ['python'], info: { platform: 'macOS', cores: 8, memoryGB: 16, memoryFreeGB: 6 },
    limits: { paused: false, threads: 6, wantThreads: 6, wantSharing: true, cap: null, mobile: false } }),
];
await settle();
assert.equal(sockets.length, 1, 'the page connects');
sockets[0].onmessage({ data: JSON.stringify({ type: 'welcome', id: 'me1', name: 'Test phone', color: '#2563C9', kind: 'browser', caps: ['admin', 'submit', 'shell', 'files'],
  limits: phoneLimits, devices, jobs: [], snapshot: null, files: [], procs: [], address: 'http://127.0.0.1:8420', joinCode: '6453' }) });
await settle(); await settle();

// it opens on Home
assert.equal($('#app').hidden, false);
assert.equal($('#win-home').hidden, false);
assert.equal($('#launcher').hidden, true);
assert.ok($('.dock-item[data-open="home"]').classList.contains('front'));
assert.ok($('.hm-dev[data-id="mac"]') && $('.hm-dev[data-id="me1"]'), 'Home lists the devices');
assert.ok(['GET /api/engine', 'GET /api/agents', 'GET /api/jobs'].every((c) => calls.includes(c)));
assert.ok(!calls.includes('GET /api/me'), 'the welcome message already said who this is');
// the phone's own panel says it takes small tasks only (it offers 4 threads, as in its welcome)
const slider = $('#me-threads');
slider.value = '4';
slider.dispatchEvent(new window.Event('input', { bubbles: true }));
slider.dispatchEvent(new window.Event('change', { bubbles: true }));
assert.equal($('#me-limit').hidden, false);
assert.match($('#me-limit').textContent, /Phones and tablets take small tasks only/);
sockets[0].onmessage({ data: JSON.stringify({ type: 'limits', ...phoneLimits, paused: true, by: 'Desk Mac' }) });
assert.match($('#me-limit').textContent, /Paused from the Home screen/);
sockets[0].onmessage({ data: JSON.stringify({ type: 'limits', ...phoneLimits, cap: 2, threads: 2 }) });
assert.match($('#me-limit').textContent, /The pool uses 2 of these threads/);
sockets[0].onmessage({ data: JSON.stringify({ type: 'limits', ...phoneLimits, cap: 4, threads: 4 }) });
assert.equal($('#me-limit').hidden, true);

// closed, Home comes back from the app tiles, the dock and the name in the top bar
$('#win-home [data-act="close"]').click();
assert.equal($('#win-home').hidden, true);
assert.equal($('#launcher').hidden, false);
$('#launcher [data-open="home"]').click();
assert.equal($('#win-home').hidden, false);
$('#win-home [data-act="min"]').click();
assert.equal($('#win-home').hidden, true);
$('#pb-home').click();
assert.equal($('#win-home').hidden, false);
$('.dock-item[data-open="batch"]').click();
await settle();
assert.equal($('#win-batch').hidden, false);
$('.dock-item[data-open="home"]').click();
assert.ok($('.dock-item[data-open="home"]').classList.contains('front'));

await settle();
assert.deepEqual(errors.map((e) => e.message), [], 'no errors while booting');
window.close();
console.log('app boot smoke: ok');
