// Headless smoke test of the Agents app (yourharness/web/agents.js) against a stubbed coordinator.
// Needs jsdom:  npm install --no-save jsdom@26   then:  node tests/web/agents_smoke.mjs
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import assert from 'node:assert/strict';

const src = fs.readFileSync(process.argv[2] || new URL('../../yourharness/web/agents.js', import.meta.url), 'utf8');
const dom = new JSDOM('<!doctype html><body><div id="ag-body"></div></body>', { url: 'http://127.0.0.1:8420/', runScripts: 'outside-only' });
const { window } = dom;
const { document } = window;
window.CSS = { escape: (s) => String(s).replace(/[^\w-]/g, '\\$&') };
window.alert = (m) => { throw new Error('alert: ' + m); };
window.confirm = () => true;
window.HTMLElement.prototype.scrollIntoView = () => {};
const calls = [];
const runs = [];
window.fetch = async (url, opts = {}) => {
  const method = opts.method || 'GET';
  const body = opts.body ? JSON.parse(opts.body) : undefined;
  calls.push({ method, url, body });
  const json = (o, status = 200) => ({ ok: status < 400, status, headers: { get: () => 'application/json' }, json: async () => o, text: async () => JSON.stringify(o) });
  const text = (t) => ({ ok: true, status: 200, headers: { get: () => 'text/plain' }, text: async () => t, json: async () => { throw new Error('not json'); } });
  if (url === '/api/agents') return json({ runs, repos: [], github: { connected: false, user: '' }, adapters: {}, isAdmin: true });
  if (url === '/api/invites' && method === 'GET') return json({ invites: [], address: 'http://192.168.1.5:8420' });
  if (url === '/api/invites' && method === 'POST') return json({ invite: { id: 'i1' }, code: 'ABCD-EFGH-JKLM-NPQR', link: 'http://192.168.1.5:8420/#join=ABCD-EFGH-JKLM-NPQR', address: 'http://192.168.1.5:8420' }, 201);
  if (url === '/api/agent-runs' && method === 'POST') {
    const run = { id: 'r1', title: body.title, repo: body.repo, baseBranch: '', baseCommit: '', resultsBranch: '', openPr: false, status: 'preparing', note: '', error: '', createdBy: 'Mac', createdAt: Date.now() / 1000, tasks: body.tasks.map((t, i) => ({ id: `t${i}`, runId: 'r1', title: t.title, adapter: t.adapter, adapterName: 'x', model: t.model, device: t.device, deviceName: '', status: 'queued', note: '', error: '', result: {}, hasDiff: false })) };
    return json({ run }, 201);
  }
  if (url.endsWith('/log')) return text('Workspace ready\n');
  throw new Error('unexpected ' + method + ' ' + url);
};
window.YH = { devices: [], address: 'http://192.168.1.5:8420', joinCode: '1234', can: () => true };
window.eval(src);

const devices = [
  { id: 'aaaaaa', name: 'Mac', kind: 'agent', owned: true, stale: false, info: { os: 'macOS' }, exec: { os: 'macOS' }, agentTasks: { on: true, slots: 1, busy: 0, agents: [{ id: 'command', name: 'Command (no AI)' }, { id: 'claude', name: 'Claude Code', version: '2.1' }] } },
  { id: 'bbbbbb', name: 'Sams PC', kind: 'agent', owned: false, stale: false, info: { os: 'Ubuntu' }, exec: null, agentTasks: { on: true, slots: 1, busy: 0, agents: [{ id: 'command' }, { id: 'codex', name: 'Codex CLI' }] } },
  { id: 'cccccc', name: 'iPhone', kind: 'browser', owned: true, stale: false, info: { platform: 'iPhone' }, agentTasks: null },
];
const msg = (m) => window.dispatchEvent(new window.CustomEvent('yh:msg', { detail: m }));
const tick = () => new Promise((r) => setTimeout(r, 0));

window.YHAgents.opened();
msg({ type: 'devices', devices });
await tick(); await tick();
assert.ok(document.querySelector('[data-tab="runs"]'), 'tabs are built');
assert.match(document.querySelector('#ag-runs').textContent, /No runs yet/);

// computers tab
document.querySelector('[data-tab="computers"]').click();
const rows = document.querySelector('#ag-comp-rows').textContent;
assert.match(rows, /Mac/); assert.match(rows, /Sams PC/); assert.match(rows, /A friend’s/); assert.match(rows, /Claude Code/);
assert.doesNotMatch(rows, /iPhone/, 'browsers are not listed as computers');
assert.match(document.querySelector('#ag-browsers').textContent, /iPhone/);
assert.equal(document.querySelector('#ag-code').value, '1234');
document.querySelector('#ag-inv-label').value = "Sam's PC";
document.querySelector('[data-act="invite"]').click();
await tick(); await tick(); await tick();
assert.match(document.querySelector('#ag-inv-new').innerHTML, /ABCD-EFGH-JKLM-NPQR/);
assert.match(document.querySelector('#ag-inv-cmd').value, /--tasks/);

// new run: the device select lists computers; picking the friend's narrows the AI choices
document.querySelector('[data-tab="new"]').click();
const card = document.querySelector('.ag-task');
const devSel = card.querySelector('[data-f="device"]');
assert.deepEqual([...devSel.options].map((o) => o.value), ['', 'aaaaaa', 'bbbbbb']);
assert.deepEqual([...card.querySelector('[data-f="adapter"]').options].map((o) => o.value), ['claude', 'command'], 'any of mine: what my own computers have');
devSel.value = 'bbbbbb';
devSel.dispatchEvent(new window.Event('change', { bubbles: true }));
assert.deepEqual([...card.querySelector('[data-f="adapter"]').options].map((o) => o.value), ['codex'], 'a friend who allowed no commands gets no Command agent');
assert.match(card.querySelector('[data-f="warn"]').textContent, /friend’s computer/);
assert.match(card.querySelector('[data-f="warn"]').textContent, /doesn’t allow commands/);
assert.equal(card.querySelector('[data-f="autonomy"]').value, 'edit', 'agents only edit files unless asked otherwise');
// a device update must not reset what was picked
msg({ type: 'devices', devices });
assert.equal(card.querySelector('[data-f="device"]').value, 'bbbbbb');

// fill in and start
document.querySelector('#ag-repo-kind').value = 'local';
document.querySelector('#ag-repo-kind').dispatchEvent(new window.Event('change', { bubbles: true }));
assert.equal(document.querySelector('#ag-local-field').hidden, false);
document.querySelector('#ag-local-repo').value = '/Users/me/project';
document.querySelector('#ag-title').value = 'Smoke run';
card.querySelector('[data-f="title"]').value = 'Write tests';
card.querySelector('[data-f="instructions"]').value = 'Add tests';
card.querySelector('[data-f="checks"]').value = 'npm test';
document.querySelector('[data-act="add-task"]').click();
const card2 = document.querySelectorAll('.ag-task')[1];
const ad2 = card2.querySelector('[data-f="adapter"]');
ad2.value = 'command';
ad2.dispatchEvent(new window.Event('change', { bubbles: true }));
assert.equal(card2.querySelector('[data-show="cmd"]').hidden, false);
assert.equal(card2.querySelector('[data-show="ai"]').hidden, true);
card2.querySelector('[data-f="title"]').value = 'List files';
card2.querySelector('[data-f="command"]').value = 'ls > files.txt';
document.querySelector('#ag-final-checks').value = 'pytest -q\nnpm test';
document.querySelector('#ag-final-gate').checked = true;
document.querySelector('#ag-form').dispatchEvent(new window.Event('submit', { bubbles: true, cancelable: true }));
await tick(); await tick(); await tick();
const post = calls.find((c) => c.method === 'POST' && c.url === '/api/agent-runs');
assert.ok(post, 'the run was posted');
assert.equal(post.body.repo.kind, 'local');
assert.equal(post.body.repo.name, '/Users/me/project');
assert.equal(post.body.tasks.length, 2);
assert.equal(post.body.tasks[0].device, 'bbbbbb');
assert.equal(post.body.tasks[0].adapter, 'codex');
assert.equal(post.body.tasks[0].instructions, 'Add tests');
assert.equal(post.body.tasks[0].command, '');
assert.equal(post.body.tasks[1].adapter, 'command');
assert.equal(post.body.tasks[1].command, 'ls > files.txt');
assert.equal(post.body.tasks[1].instructions, '');
assert.equal(post.body.final_checks, 'pytest -q\nnpm test');
assert.equal(post.body.final_gate, true);
assert.equal(post.body.final_with_friends, false, 'a friend’s code never runs on your computer unless you say so');
assert.equal(document.querySelector('[data-pane="runs"]').hidden, false, 'switches to Runs after starting');

// live updates
const run = { id: 'r1', title: 'Smoke run', repo: { kind: 'local', name: '/Users/me/project' }, baseBranch: 'main', baseCommit: 'abc', resultsBranch: 'yh/r1', openPr: false, status: 'running', note: '', error: '', createdBy: 'Mac', createdAt: Date.now() / 1000, prUrl: 'javascript:alert(1)', branchUrl: null,
  tasks: [{ id: 't0', runId: 'r1', title: 'Write tests', adapter: 'codex', adapterName: 'Codex CLI', model: '', device: 'bbbbbb', deviceName: 'Sams PC', status: 'running', phase: 'agent', note: 'Codex CLI', error: '', result: {}, hasDiff: false, startedAt: Date.now() / 1000 }] };
msg({ type: 'agents.run', run });
let html = document.querySelector('#ag-runs').innerHTML;
assert.match(html, /Sams PC/); assert.match(html, /Cancel run/);
assert.doesNotMatch(html, /javascript:/, 'only https links are rendered');
msg({ type: 'agents.task', task: { ...run.tasks[0], status: 'failed', error: 'Not accepted. Looks like a secret: a.txt:1', canAccept: true, hasDiff: true } });
html = document.querySelector('#ag-runs').innerHTML;
assert.match(html, /Accept anyway/); assert.match(html, /Retry/); assert.match(html, /Changes/);
document.querySelector('[data-act="log"][data-id="t0"]').click();
await tick(); await tick(); await tick();
assert.equal(document.querySelector('#ag-viewer').hidden, false);
assert.match(document.querySelector('#ag-viewer-text').textContent, /Workspace ready/);
msg({ type: 'agents.log', task: 't0', data: 'more output\n' });
assert.match(document.querySelector('#ag-viewer-text').textContent, /more output/);
msg({ type: 'agents.run', run: { ...run, title: '<img src=x onerror=alert(1)>' } });
assert.ok(!document.querySelector('#ag-runs img'), 'titles are escaped');
// final checks on the combined branch
msg({ type: 'agents.run', run: { ...run, final: { checks: ['pytest -q'] }, validation: { status: 'failed', commit: 'abc', checks: [{ cmd: '<b>pytest</b>', code: 1 }] },
  validationText: 'Not validated: the combined branch failed its final checks (pytest).' } });
html = document.querySelector('#ag-runs').textContent;
assert.match(html, /Not validated: the combined branch failed/);
assert.ok(!document.querySelector('#ag-runs code b'), 'commands are escaped');
document.querySelector('[data-act="run-log"][data-id="r1"]').click();
await tick(); await tick(); await tick();
assert.ok(calls.find((c) => c.url === '/api/agent-runs/r1/log'), 'the final checks’ output can be read');
assert.match(document.querySelector('#ag-viewer-h').textContent, /Final checks/);
console.log('agents.js smoke test passed:', calls.length, 'requests');
