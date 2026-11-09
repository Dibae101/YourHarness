'use strict';
// The Agents app: pick computers (your own, and friends' that allow it), choose
// the AI agent and model for each task, say where the results go on GitHub, and
// watch runs as they happen. Everything here talks to /api/agents* on the
// coordinator; live updates arrive as "agents.*" messages relayed by app.js.
(() => {
  const $ = (sel, el = document) => el.querySelector(sel);
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const safeUrl = (u) => (/^https:\/\//.test(String(u || '')) ? String(u) : '');
  const ago = (t) => {
    if (!t) return '';
    const s = Math.max(0, Date.now() / 1000 - t);
    if (s < 60) return 'just now';
    if (s < 3600) return `${Math.round(s / 60)} min ago`;
    if (s < 86400) return `${Math.round(s / 3600)} h ago`;
    return new Date(t * 1000).toLocaleDateString();
  };
  const dur = (a, b) => {
    if (!a) return '';
    const s = Math.max(0, (b || Date.now() / 1000) - a);
    return s < 60 ? `${Math.round(s)} s` : s < 3600 ? `${Math.floor(s / 60)} min ${Math.round(s % 60)} s` : `${(s / 3600).toFixed(1)} h`;
  };

  const ADAPTER_NAMES = { command: 'Command (no AI)', claude: 'Claude Code', codex: 'Codex CLI', kiro: 'Kiro CLI' };
  const MODEL_HINTS = { claude: ['sonnet', 'opus', 'haiku'], codex: [], kiro: [], command: [] };
  const STATUS = {
    preparing: 'Getting ready', running: 'Running', integrating: 'Combining', done: 'Done', failed: 'Failed', cancelled: 'Cancelled',
    queued: 'Waiting', verifying: 'Checking',
  };

  const A = {
    loaded: false,
    runs: new Map(), // id -> run (with tasks)
    devices: [],
    github: { connected: false, user: '' },
    repos: [],
    isAdmin: false,
    ghRepos: null,
    viewer: null, // { taskId, mode: 'log' | 'diff', title }
    tab: 'runs',
    invites: [],
  };

  // ---------------------------------------------------------------- talking to the coordinator
  async function api(method, path, body) {
    const opts = { method, credentials: 'same-origin', headers: {} };
    if (body !== undefined) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
    const res = await fetch(path, opts);
    const type = res.headers.get('Content-Type') || '';
    const data = type.includes('application/json') ? await res.json() : await res.text();
    if (!res.ok) throw new Error((data && data.error) || `The coordinator answered ${res.status}.`);
    return data;
  }

  async function load() {
    try {
      const data = await api('GET', '/api/agents');
      A.runs = new Map(data.runs.map((r) => [r.id, r]));
      A.github = data.github;
      A.repos = data.repos || [];
      A.isAdmin = !!data.isAdmin;
      A.loaded = true;
      renderAll();
      if (A.isAdmin) loadInvites();
      if (A.github.connected && !A.ghRepos) loadGitHubRepos();
    } catch (e) {
      $('#ag-runs').innerHTML = `<p class="error">${esc(e.message)}</p>`;
    }
  }

  async function loadInvites() {
    try { A.invites = (await api('GET', '/api/invites')).invites; renderInvites(); } catch { /* not an admin */ }
  }

  async function loadGitHubRepos() {
    try { A.ghRepos = (await api('GET', '/api/github/repos')).repos; } catch (e) { A.ghRepos = []; flash('#ag-gh-status', e.message, true); }
    renderRepoChoices();
    renderGitHub();
  }

  function flash(sel, text, bad) {
    const el = $(sel);
    if (!el) return;
    el.textContent = text;
    el.classList.toggle('error', !!bad);
  }

  // ---------------------------------------------------------------- the window's frame
  function build() {
    const body = $('#ag-body');
    body.innerHTML = `
      <nav class="ag-tabs" role="tablist" aria-label="Agents">
        <button type="button" role="tab" data-tab="runs">Runs</button>
        <button type="button" role="tab" data-tab="new">New run</button>
        <button type="button" role="tab" data-tab="computers">Computers</button>
        <button type="button" role="tab" data-tab="github">GitHub</button>
      </nav>
      <section class="ag-pane" data-pane="runs">
        <div id="ag-runs" class="stack"><p class="hint">Loading…</p></div>
        <section id="ag-viewer" class="panel ag-viewer" hidden>
          <div class="panel-head"><h2 id="ag-viewer-h">Log</h2><button class="btn ghost small" type="button" data-act="close-viewer">Close</button></div>
          <pre id="ag-viewer-text" class="ag-log" tabindex="0"></pre>
        </section>
      </section>
      <section class="ag-pane" data-pane="new" hidden>
        <form id="ag-form" class="stack" novalidate>
          <section class="panel">
            <h2>Where the results go</h2>
            <div class="ag-grid">
              <label class="field"><span>Run name</span><input id="ag-title" maxlength="120" placeholder="For example: Login page and its tests"></label>
              <label class="field"><span>Repository</span>
                <select id="ag-repo-kind"><option value="github">A GitHub repository</option><option value="local">A Git repository on the coordinator's computer</option></select></label>
              <label class="field" id="ag-gh-field"><span>GitHub repository (owner/name)</span><input id="ag-gh-repo" list="ag-gh-list" autocomplete="off" placeholder="owner/name"><datalist id="ag-gh-list"></datalist></label>
              <label class="field" id="ag-local-field" hidden><span>Path on the coordinator's computer</span><input id="ag-local-repo" autocomplete="off" placeholder="/Users/you/code/project"></label>
              <label class="field"><span>Start from branch</span><input id="ag-base" list="ag-branch-list" autocomplete="off" placeholder="the default branch"><datalist id="ag-branch-list"></datalist></label>
              <label class="field"><span>Push results to branch</span><input id="ag-results" autocomplete="off" placeholder="yh/… (made for this run)"></label>
            </div>
            <label class="switch ag-switch" id="ag-pr-field"><input id="ag-pr" type="checkbox" checked><span class="switch-track" aria-hidden="true"></span><span>Open a draft pull request</span></label>
            <p class="hint" id="ag-gh-hint"></p>
          </section>
          <div id="ag-tasks" class="stack"></div>
          <div class="ag-actions">
            <button class="btn ghost" type="button" data-act="add-task">Add a task</button>
            <button class="btn primary" type="submit">Start the run</button>
          </div>
          <p id="ag-form-status" class="hint" aria-live="polite"></p>
        </form>
      </section>
      <section class="ag-pane" data-pane="computers" hidden>
        <section class="panel">
          <h2>Computers in the pool</h2>
          <p class="hint">Each task runs on one computer, in a copy of the repository of its own. Pick the computer and the AI in “New run”.</p>
          <div class="scroll-x"><table class="res-table ag-table"><thead><tr><th>Computer</th><th>Whose</th><th>Agent tasks</th><th>AI agents found</th><th>Busy</th></tr></thead><tbody id="ag-comp-rows"></tbody></table></div>
          <p class="hint" id="ag-browsers"></p>
        </section>
        <div class="grid-2">
          <section class="panel">
            <h2>Add one of your computers</h2>
            <p class="hint">On your own computers, open the YourHarness app and join with the address and code below, or run the command. Your own computers take agent tasks unless started with <code>--no-tasks</code>.</p>
            <div class="field"><span>Pool address</span><div class="copyrow"><input id="ag-addr" readonly><button class="btn ghost small" type="button" data-copy="ag-addr">Copy</button></div></div>
            <div class="field"><span>Join code (your devices only)</span><div class="copyrow"><input id="ag-code" class="code-big" readonly><button class="btn ghost small" type="button" data-copy="ag-code">Copy</button></div></div>
            <div class="field"><span>Mac or Linux terminal</span><div class="copyrow"><textarea id="ag-own-cmd" rows="2" readonly></textarea><button class="btn ghost small" type="button" data-copy="ag-own-cmd">Copy</button></div></div>
          </section>
          <section class="panel" id="ag-invite-panel">
            <h2>Invite a friend's computer</h2>
            <p class="hint">A friend code is good for one computer and expires. It lets that computer take part, but never control the pool: it can't open terminals, read files, or see your GitHub token. It runs agent tasks only if your friend allows them on that computer.</p>
            <div class="copyrow"><input id="ag-inv-label" maxlength="60" placeholder="Whose computer, e.g. Sam's PC"><button class="btn primary small" type="button" data-act="invite">Make a code</button></div>
            <div id="ag-inv-new" class="ag-invite" hidden></div>
            <ul id="ag-inv-list" class="ag-inv-list"></ul>
            <p class="hint">Their computer must be able to reach this pool's address. On a different network, connect both through something like Tailscale and start the pool with <code>--public-url</code>.</p>
          </section>
        </div>
      </section>
      <section class="ag-pane" data-pane="github" hidden>
        <section class="panel">
          <h2>GitHub</h2>
          <p id="ag-gh-status" class="hint"></p>
          <div id="ag-gh-connect">
            <p class="hint">Make a <b>fine-grained personal access token</b> on GitHub (Settings → Developer settings → Personal access tokens → Fine-grained tokens). Under <b>Repository access</b>, pick only the repositories YourHarness should push to. Under <b>Permissions</b>, set <b>Contents</b> and <b>Pull requests</b> to <b>Read and write</b>.</p>
            <div class="copyrow"><input id="ag-gh-token" type="password" autocomplete="off" spellcheck="false" placeholder="github_pat_…"><button class="btn primary small" type="button" data-act="gh-save">Connect</button></div>
            <p class="hint">The token stays on the coordinator's computer, in a file only its user can read. It is never sent to the other computers, which only ever talk to the coordinator. Results are pushed to a new branch, never to main.</p>
          </div>
          <button class="btn ghost small" type="button" data-act="gh-disconnect" id="ag-gh-off" hidden>Disconnect GitHub</button>
          <div id="ag-gh-repos"></div>
        </section>
      </section>`;
    addTask();
    body.addEventListener('click', onClick);
    body.addEventListener('change', onChange);
    body.addEventListener('input', onInput);
    $('#ag-form').addEventListener('submit', submitRun);
  }

  function showTab(tab) {
    A.tab = tab;
    document.querySelectorAll('#ag-body [data-tab]').forEach((b) => b.setAttribute('aria-selected', String(b.dataset.tab === tab)));
    document.querySelectorAll('#ag-body [data-pane]').forEach((p) => { p.hidden = p.dataset.pane !== tab; });
    if (tab === 'computers' && A.isAdmin) loadInvites();
  }

  // ---------------------------------------------------------------- computers
  const agentsOf = (d) => ((d.agentTasks && d.agentTasks.agents) || []);
  const computers = () => A.devices.filter((d) => d.kind === 'agent');

  function deviceLabel(d) {
    const t = d.agentTasks || {};
    const who = d.owned ? '' : ' (friend)';
    const state = d.stale ? 'not responding' : !t.on ? 'tasks off' : t.busy >= (t.slots || 1) ? 'busy' : 'ready';
    return `${d.name}${who} · ${state}`;
  }

  function renderComputers() {
    const rows = computers().map((d) => {
      const t = d.agentTasks || {};
      const status = d.stale ? '<span class="ag-badge warn">Not responding</span>'
        : t.on ? '<span class="ag-badge ok">Ready</span>' : `<span class="ag-badge">Off</span> <span class="hint">${esc(t.why || '')}</span>`;
      const chips = agentsOf(d).map((a) => `<span class="ag-chip" title="${esc(a.version || '')}">${esc(a.name || ADAPTER_NAMES[a.id] || a.id)}</span>`).join(' ');
      return `<tr><td><b>${esc(d.name)}</b><br><span class="hint">${esc((d.exec && d.exec.os) || d.info.os || d.info.platform || '')}</span></td>
        <td>${d.owned ? 'Yours' : 'A friend’s'}</td><td>${status}</td><td>${chips || '<span class="hint">none</span>'}</td>
        <td>${t.on ? `${t.busy || 0} of ${t.slots || 1}` : ''}</td></tr>`;
    });
    $('#ag-comp-rows').innerHTML = rows.join('') || '<tr><td colspan="5" class="hint">No computers have joined yet.</td></tr>';
    const browsers = A.devices.filter((d) => d.kind !== 'agent').map((d) => d.name);
    $('#ag-browsers').textContent = browsers.length
      ? `Phones and browser tabs (${browsers.join(', ')}) can start and watch runs, but don't run agent tasks.` : '';
    const Y = window.YH || {};
    const addr = Y.address || location.origin;
    $('#ag-addr').value = addr;
    $('#ag-code').value = Y.joinCode || '';
    $('#ag-own-cmd').value = Y.joinCode ? `curl -fsSL ${addr}/install.sh | bash -s -- --join ${Y.joinCode} --workspace` : '';
    $('#ag-invite-panel').hidden = !A.isAdmin;
  }

  function renderInvites() {
    const list = $('#ag-inv-list');
    if (!list) return;
    list.innerHTML = A.invites.map((i) => `<li><span><b>${esc(i.label)}</b> · ${i.state === 'open' ? `open until ${esc(new Date(i.expires * 1000).toLocaleString())}` : esc(i.state)}</span>
      ${i.state === 'open' ? `<button class="btn ghost small" type="button" data-act="revoke" data-id="${esc(i.id)}">Revoke</button>` : ''}</li>`).join('');
  }

  async function makeInvite() {
    const label = $('#ag-inv-label').value.trim() || 'A friend';
    try {
      const r = await api('POST', '/api/invites', { label, hours: 24 });
      const box = $('#ag-inv-new');
      const addr = r.address || location.origin;
      box.hidden = false;
      box.innerHTML = `
        <p>Send this to <b>${esc(label)}</b>. It works once, for 24 hours.</p>
        <div class="field"><span>Friend code</span><div class="copyrow"><input id="ag-inv-code" class="code-big" readonly value="${esc(r.code)}"><button class="btn ghost small" type="button" data-copy="ag-inv-code">Copy</button></div></div>
        <div class="field"><span>Link (opens the pool in a browser and joins)</span><div class="copyrow"><input id="ag-inv-link" readonly value="${esc(r.link)}"><button class="btn ghost small" type="button" data-copy="ag-inv-link">Copy</button></div></div>
        <div class="field"><span>Their Mac or Linux computer, in a terminal (needs Python 3.10 or newer, and git for agent tasks)</span><div class="copyrow"><textarea id="ag-inv-cmd" rows="3" readonly>curl -fsSL ${esc(addr)}/install.sh | bash -s -- --join ${esc(r.code)} --tasks</textarea><button class="btn ghost small" type="button" data-copy="ag-inv-cmd">Copy</button></div></div>
        <div class="field"><span>Their Windows PC, in PowerShell (sets up Ubuntu, then asks for the code and whether to allow agent tasks)</span><div class="copyrow"><input id="ag-inv-win" readonly value="irm ${esc(addr)}/install.ps1 | iex"><button class="btn ghost small" type="button" data-copy="ag-inv-win">Copy</button></div></div>
        <p class="hint">Leave out <code>--tasks</code> if they only want to lend compute. The YourHarness desktop app takes friend codes from its next release (File → Join another pool); the v0.7 apps don't.</p>`;
      loadInvites();
    } catch (e) { alert(e.message); }
  }

  // ---------------------------------------------------------------- GitHub
  function renderGitHub() {
    const g = A.github;
    flash('#ag-gh-status', g.connected ? `Connected as ${g.user || 'a GitHub user'}.` : 'Not connected. Runs can still use a Git repository on the coordinator’s computer.');
    $('#ag-gh-connect').hidden = g.connected || !A.isAdmin;
    $('#ag-gh-off').hidden = !g.connected || !A.isAdmin;
    const repos = A.ghRepos || [];
    $('#ag-gh-repos').innerHTML = g.connected && repos.length ? `<h3 class="ag-h3">Repositories this token can see</h3><ul class="ag-inv-list">${repos.map((r) =>
      `<li><span><b>${esc(r.name)}</b> ${r.private ? '· private' : ''} · default branch ${esc(r.defaultBranch || '?')}</span>${r.canPush ? '' : '<span class="hint">read only</span>'}</li>`).join('')}</ul>` : '';
    $('#ag-gh-hint').textContent = g.connected ? '' : 'GitHub isn’t connected yet: add a token in the GitHub tab, or use a repository on the coordinator’s computer.';
  }

  function renderRepoChoices() {
    const list = $('#ag-gh-list');
    if (list) list.innerHTML = (A.ghRepos || []).map((r) => `<option value="${esc(r.name)}">`).join('');
  }

  async function loadBranches() {
    const kind = $('#ag-repo-kind').value;
    const name = $('#ag-gh-repo').value.trim();
    const list = $('#ag-branch-list');
    list.innerHTML = '';
    if (kind !== 'github' || !/^[\w.-]+\/[\w.-]+$/.test(name) || !A.github.connected) return;
    try {
      const { branches } = await api('GET', `/api/github/branches?repo=${encodeURIComponent(name)}`);
      list.innerHTML = branches.map((b) => `<option value="${esc(b)}">`).join('');
    } catch { /* the field still takes typing */ }
  }

  // ---------------------------------------------------------------- the task editor
  let taskSeq = 0;

  function taskCard(prefill = {}) {
    const n = ++taskSeq;
    const el = document.createElement('section');
    el.className = 'panel ag-task';
    el.dataset.n = String(n);
    el.innerHTML = `
      <div class="panel-head"><h2>Task</h2><button class="btn ghost small danger" type="button" data-act="remove-task">Remove</button></div>
      <div class="ag-grid">
        <label class="field"><span>Title</span><input data-f="title" maxlength="120" placeholder="What this task does"></label>
        <label class="field"><span>Kind of work</span><select data-f="kind"><option value="code">A code change</option><option value="general">Other work: a report, files, research</option></select></label>
        <label class="field"><span>Computer</span><select data-f="device"></select></label>
        <label class="field"><span>Done by</span><select data-f="adapter"></select></label>
        <label class="field" data-show="ai"><span>Model</span><input data-f="model" maxlength="100" autocomplete="off" placeholder="the agent's default"><datalist></datalist></label>
        <label class="field" data-show="ai"><span>The agent may</span><select data-f="autonomy"><option value="full">Edit files and run commands</option><option value="edit">Only edit files</option></select></label>
      </div>
      <label class="field" data-show="ai"><span>Instructions for the agent</span><textarea data-f="instructions" rows="5" placeholder="Describe the change, what done looks like, and anything to avoid."></textarea></label>
      <label class="field" data-show="cmd" hidden><span>Command to run in the repository (bash)</span><textarea data-f="command" rows="3" placeholder="npm test > results/test-report.txt"></textarea></label>
      <p class="hint ag-warn" data-f="warn" hidden></p>
      <details class="ag-more"><summary>Checks, files it may change, limits</summary>
        <div class="ag-grid">
          <label class="field"><span>Checks that must pass (one command per line)</span><textarea data-f="checks" rows="3" placeholder="npm test"></textarea></label>
          <label class="field"><span>Setup commands first (one per line)</span><textarea data-f="setup" rows="3" placeholder="npm ci"></textarea></label>
          <label class="field"><span>Files it may change (one pattern per line; empty means any)</span><textarea data-f="scope" rows="3" placeholder="src/auth/**"></textarea></label>
          <label class="field"><span>Files it must not change (one pattern per line)</span><textarea data-f="deny" rows="3" placeholder="package-lock.json"></textarea></label>
          <label class="field"><span>Time limit (minutes)</span><input data-f="time_limit_min" type="number" min="1" max="720" value="30"></label>
          <label class="field" data-show="claude"><span>Most it may spend (US$, Claude Code)</span><input data-f="max_usd" type="number" min="0" step="0.5" placeholder="no limit"></label>
          <label class="field" data-show="ai"><span>Rounds when checks fail</span><input data-f="rounds" type="number" min="1" max="5" value="2"></label>
        </div>
        <label class="switch ag-switch"><input data-f="allow_workflows" type="checkbox"><span class="switch-track" aria-hidden="true"></span><span>Allow changes to CI workflows (.github/workflows)</span></label>
      </details>`;
    fillDeviceSelect(el);
    fillAdapterSelect(el);
    const set = (f, v) => { const i = el.querySelector(`[data-f="${f}"]`); if (!i || v == null) return; if (i.type === 'checkbox') i.checked = !!v; else i.value = Array.isArray(v) ? v.join('\n') : v; };
    Object.entries(prefill).forEach(([k, v]) => { if (k !== 'device' && k !== 'adapter') set(k, v); });
    if (prefill.device != null) { set('device', prefill.device); fillAdapterSelect(el); }
    if (prefill.adapter) { set('adapter', prefill.adapter); el.querySelector('[data-f="adapter"]').dataset.chosen = '1'; }
    updateTaskCard(el);
    return el;
  }

  function addTask(prefill) {
    const card = taskCard(prefill);
    $('#ag-tasks').appendChild(card);
    renumber();
    return card;
  }

  function renumber() {
    document.querySelectorAll('#ag-tasks .ag-task').forEach((el, i) => { $('h2', el).textContent = `Task ${i + 1}`; });
  }

  function fillDeviceSelect(card) {
    const sel = card.querySelector('[data-f="device"]');
    const opts = [['', 'Any of my computers']].concat(computers().map((d) => [d.id, deviceLabel(d)]));
    const sig = JSON.stringify(opts);
    if (sel.dataset.sig === sig) return;
    const keep = sel.value;
    sel.innerHTML = opts.map(([v, t]) => `<option value="${esc(v)}">${esc(t)}</option>`).join('');
    sel.dataset.sig = sig;
    if (opts.some(([v]) => v === keep)) sel.value = keep;
  }

  function adaptersFor(deviceId) {
    const ids = new Set(['command']);
    const devs = deviceId ? computers().filter((d) => d.id === deviceId) : computers().filter((d) => d.owned);
    devs.forEach((d) => agentsOf(d).forEach((a) => ids.add(a.id)));
    return ['claude', 'codex', 'kiro', 'command'].filter((id) => ids.has(id));
  }

  function fillAdapterSelect(card) {
    const sel = card.querySelector('[data-f="adapter"]');
    const ids = adaptersFor(card.querySelector('[data-f="device"]').value);
    const sig = ids.join(',');
    if (sel.dataset.sig === sig) return;
    // until you pick one yourself, default to the first AI agent the computer has
    const firstAi = ids.find((id) => id !== 'command') || 'command';
    const keep = sel.dataset.chosen ? sel.value : firstAi;
    sel.innerHTML = ids.map((id) => `<option value="${id}">${esc(ADAPTER_NAMES[id])}</option>`).join('');
    sel.dataset.sig = sig;
    sel.value = ids.includes(keep) ? keep : firstAi;
  }

  function updateTaskCard(card) {
    const adapter = card.querySelector('[data-f="adapter"]').value;
    const ai = adapter !== 'command';
    card.querySelectorAll('[data-show="ai"]').forEach((el) => { el.hidden = !ai; });
    card.querySelectorAll('[data-show="cmd"]').forEach((el) => { el.hidden = ai; });
    card.querySelectorAll('[data-show="claude"]').forEach((el) => { el.hidden = adapter !== 'claude'; });
    const model = card.querySelector('[data-f="model"]');
    const dl = model.nextElementSibling;
    dl.id = `ag-models-${card.dataset.n}`;
    model.setAttribute('list', dl.id);
    dl.innerHTML = (MODEL_HINTS[adapter] || []).map((m) => `<option value="${esc(m)}">`).join('');
    const devId = card.querySelector('[data-f="device"]').value;
    const d = computers().find((x) => x.id === devId);
    const warn = card.querySelector('[data-f="warn"]');
    const notes = [];
    if (d && !d.owned) notes.push(`${d.name} is a friend’s computer: this task sends it the repository’s code, and the agent runs there with your friend’s account and AI subscription.`);
    if (d && d.agentTasks && !d.agentTasks.on) notes.push(`${d.name} can’t run agent tasks right now: ${d.agentTasks.why || 'it has them switched off'}. The task will wait.`);
    if (!devId && !computers().some((x) => x.owned && x.agentTasks && x.agentTasks.on)) notes.push('None of your computers can run agent tasks right now, so the task will wait until one can.');
    warn.hidden = !notes.length;
    warn.textContent = notes.join(' ');
  }

  function readTask(card) {
    const v = (f) => { const el = card.querySelector(`[data-f="${f}"]`); return el ? (el.type === 'checkbox' ? el.checked : el.value) : ''; };
    const t = {
      title: v('title').trim(), kind: v('kind'), device: v('device'), adapter: v('adapter'), model: v('model').trim(), autonomy: v('autonomy'),
      instructions: v('instructions'), command: v('command'), checks: v('checks'), setup: v('setup'), scope: v('scope'), deny: v('deny'),
      time_limit_min: Number(v('time_limit_min')) || 30, rounds: Number(v('rounds')) || 2, allow_workflows: v('allow_workflows'),
    };
    if (t.adapter === 'claude' && v('max_usd')) t.max_usd = Number(v('max_usd'));
    if (t.adapter === 'command') { t.instructions = ''; t.model = ''; } else t.command = '';
    return t;
  }

  async function submitRun(e) {
    e.preventDefault();
    const kind = $('#ag-repo-kind').value;
    const body = {
      title: $('#ag-title').value.trim(),
      repo: { kind, name: (kind === 'github' ? $('#ag-gh-repo') : $('#ag-local-repo')).value.trim() },
      base_branch: $('#ag-base').value.trim(),
      results_branch: $('#ag-results').value.trim(),
      open_pr: kind === 'github' && $('#ag-pr').checked,
      tasks: [...document.querySelectorAll('#ag-tasks .ag-task')].map(readTask),
    };
    const btn = $('#ag-form button[type="submit"]');
    btn.disabled = true;
    flash('#ag-form-status', 'Starting…');
    try {
      const { run } = await api('POST', '/api/agent-runs', body);
      A.runs.set(run.id, run);
      flash('#ag-form-status', 'Started. Follow it in Runs.');
      renderRuns();
      showTab('runs');
    } catch (err) {
      flash('#ag-form-status', err.message, true);
    } finally {
      btn.disabled = false;
    }
  }

  function runAgain(runId) {
    const run = A.runs.get(runId);
    if (!run) return;
    api('GET', `/api/agent-runs/${encodeURIComponent(runId)}`).then(({ run: full }) => {
      $('#ag-title').value = full.title;
      $('#ag-repo-kind').value = full.repo.kind;
      (full.repo.kind === 'github' ? $('#ag-gh-repo') : $('#ag-local-repo')).value = full.repo.name;
      $('#ag-base').value = full.baseBranch || '';
      $('#ag-results').value = '';
      $('#ag-pr').checked = full.openPr;
      onRepoKind();
      $('#ag-tasks').innerHTML = '';
      full.tasks.forEach((t) => { const s = t.spec || {}; addTask({ ...s, title: s.title || t.title, scope: s.kind === 'general' ? [] : s.scope }); });
      flash('#ag-form-status', 'Copied from the earlier run. Change anything, then start it.');
      showTab('new');
    }).catch((err) => alert(err.message));
  }

  function onRepoKind() {
    const gh = $('#ag-repo-kind').value === 'github';
    $('#ag-gh-field').hidden = !gh;
    $('#ag-local-field').hidden = gh;
    $('#ag-pr-field').hidden = !gh;
  }

  // ---------------------------------------------------------------- runs
  function badge(status) {
    const cls = { done: 'ok', failed: 'bad', cancelled: '', running: 'live', integrating: 'live', verifying: 'live', preparing: 'live', queued: '' }[status] || '';
    return `<span class="ag-badge ${cls}">${esc(STATUS[status] || status)}</span>`;
  }

  function taskRow(t) {
    const r = t.result || {};
    const bits = [];
    if (t.status === 'running' && t.phase) bits.push(esc(t.phase));
    if (t.note) bits.push(esc(t.note));
    if (t.error) bits.push(`<span class="error">${esc(t.error)}</span>`);
    if (r.checks && r.checks.length) bits.push(r.checks.map((c) => `<code>${esc(c.cmd)}</code> ${c.code === 0 ? '✓' : `✗ ${c.code}`}`).join(' · '));
    if (r.usd != null) bits.push(`$${Number(r.usd).toFixed(2)}`);
    if (t.integrated) bits.push('in the results branch');
    const acts = [`<button class="btn ghost small" type="button" data-act="log" data-id="${esc(t.id)}">Log</button>`];
    if (t.hasDiff) acts.push(`<button class="btn ghost small" type="button" data-act="diff" data-id="${esc(t.id)}">Changes</button>`);
    if (['queued', 'running', 'verifying'].includes(t.status)) acts.push(`<button class="btn ghost small" type="button" data-act="cancel-task" data-id="${esc(t.id)}">Cancel</button>`);
    if (['failed', 'cancelled'].includes(t.status)) acts.push(`<button class="btn ghost small" type="button" data-act="retry" data-id="${esc(t.id)}">Retry</button>`);
    if (t.canAccept) acts.push(`<button class="btn ghost small" type="button" data-act="accept" data-id="${esc(t.id)}" title="Push it although the checks flagged it">Accept anyway</button>`);
    const agent = esc(t.adapterName || t.adapter) + (t.model ? ` <span class="hint">${esc(t.model)}</span>` : '');
    const time = t.startedAt ? `<span class="hint">${esc(dur(t.startedAt, t.finishedAt))}</span>` : '';
    return `<tr><td><b>${esc(t.title)}</b><br>${time}</td><td>${esc(t.deviceName || (t.device ? 'picked computer' : 'any of mine'))}</td><td>${agent}</td>
      <td>${badge(t.status)}<div class="ag-detail">${bits.join('<br>')}</div></td><td class="ag-acts">${acts.join(' ')}</td></tr>`;
  }

  function runCard(run) {
    const links = [];
    const branch = safeUrl(run.branchUrl);
    const pr = safeUrl(run.prUrl);
    if (pr) links.push(`<a href="${esc(pr)}" target="_blank" rel="noopener">Pull request</a>`);
    if (branch) links.push(`<a href="${esc(branch)}" target="_blank" rel="noopener">Branch ${esc(run.resultsBranch)}</a>`);
    else if (run.resultsBranch) links.push(`branch <code>${esc(run.resultsBranch)}</code>`);
    const acts = [];
    if (!['cancelled', 'preparing'].includes(run.status) && run.baseCommit) acts.push(`<button class="btn ghost small" type="button" data-act="push" data-id="${esc(run.id)}">Push now</button>`);
    if (['preparing', 'running', 'integrating'].includes(run.status)) acts.push(`<button class="btn ghost small danger" type="button" data-act="cancel-run" data-id="${esc(run.id)}">Cancel run</button>`);
    acts.push(`<button class="btn ghost small" type="button" data-act="again" data-id="${esc(run.id)}">Run again</button>`);
    return `<section class="panel ag-run" data-run="${esc(run.id)}">
      <div class="panel-head"><h2>${esc(run.title)} ${badge(run.status)}</h2><div class="ag-acts">${acts.join(' ')}</div></div>
      <p class="hint">${esc(run.repo.name)}${run.baseBranch ? ` · from <code>${esc(run.baseBranch)}</code>` : ''} · ${links.join(' · ')} · started ${esc(ago(run.createdAt))} by ${esc(run.createdBy || '?')}</p>
      ${run.note ? `<p>${esc(run.note)}</p>` : ''}${run.error ? `<p class="error">${esc(run.error)}</p>` : ''}
      <div class="scroll-x"><table class="res-table ag-table"><thead><tr><th>Task</th><th>Computer</th><th>Agent</th><th>Status</th><th><span class="sr-only">Actions</span></th></tr></thead>
      <tbody>${run.tasks.map(taskRow).join('')}</tbody></table></div></section>`;
  }

  function renderRuns() {
    const box = $('#ag-runs');
    if (!box) return;
    const runs = [...A.runs.values()].sort((a, b) => b.createdAt - a.createdAt);
    box.innerHTML = runs.length ? runs.map(runCard).join('')
      : '<section class="panel"><h2>No runs yet</h2><p class="hint">Start one in “New run”: give each task to a computer and pick the AI agent that does it. Try “Command (no AI)” first to check everything works.</p></section>';
  }

  function renderRun(id) {
    const run = A.runs.get(id);
    const old = document.querySelector(`#ag-runs [data-run="${CSS.escape(id)}"]`);
    if (!run || !old) { renderRuns(); return; }
    const tmp = document.createElement('div');
    tmp.innerHTML = runCard(run);
    old.replaceWith(tmp.firstElementChild);
  }

  async function openViewer(taskId, mode) {
    const t = findTask(taskId);
    A.viewer = { taskId, mode };
    $('#ag-viewer').hidden = false;
    $('#ag-viewer-h').textContent = `${mode === 'diff' ? 'Changes' : 'Log'}: ${t ? t.title : taskId}`;
    const pre = $('#ag-viewer-text');
    pre.textContent = 'Loading…';
    try {
      const text = await api('GET', `/api/agent-tasks/${encodeURIComponent(taskId)}/${mode}`);
      if (A.viewer && A.viewer.taskId === taskId && A.viewer.mode === mode) {
        pre.textContent = text || (mode === 'diff' ? 'No changes.' : 'Nothing logged yet.');
        pre.scrollTop = pre.scrollHeight;
      }
    } catch (e) { pre.textContent = e.message; }
    $('#ag-viewer').scrollIntoView({ block: 'nearest' });
  }

  function findTask(id) {
    for (const r of A.runs.values()) { const t = r.tasks.find((x) => x.id === id); if (t) return t; }
    return null;
  }

  // ---------------------------------------------------------------- events
  async function act(fn) {
    try { await fn(); } catch (e) { alert(e.message); }
  }

  function onClick(e) {
    const tab = e.target.closest('[data-tab]');
    if (tab) { showTab(tab.dataset.tab); return; }
    const copy = e.target.closest('[data-copy]');
    if (copy) {
      const el = document.getElementById(copy.dataset.copy);
      if (el) { el.select(); navigator.clipboard && navigator.clipboard.writeText(el.value).catch(() => document.execCommand('copy')); copy.textContent = 'Copied'; setTimeout(() => { copy.textContent = 'Copy'; }, 1200); }
      return;
    }
    const b = e.target.closest('[data-act]');
    if (!b) return;
    const id = b.dataset.id;
    switch (b.dataset.act) {
      case 'add-task': addTask(); break;
      case 'remove-task': if (document.querySelectorAll('#ag-tasks .ag-task').length > 1) { b.closest('.ag-task').remove(); renumber(); } break;
      case 'log': openViewer(id, 'log'); break;
      case 'diff': openViewer(id, 'diff'); break;
      case 'close-viewer': A.viewer = null; $('#ag-viewer').hidden = true; break;
      case 'cancel-task': act(() => api('POST', `/api/agent-tasks/${encodeURIComponent(id)}/cancel`, {})); break;
      case 'retry': act(() => api('POST', `/api/agent-tasks/${encodeURIComponent(id)}/retry`, {})); break;
      case 'accept':
        if (confirm('Push this result although the checks flagged it? Read its changes first.')) act(() => api('POST', `/api/agent-tasks/${encodeURIComponent(id)}/accept`, {}));
        break;
      case 'push': act(() => api('POST', `/api/agent-runs/${encodeURIComponent(id)}/push`, {})); break;
      case 'cancel-run': if (confirm('Cancel every task in this run that hasn’t finished?')) act(() => api('POST', `/api/agent-runs/${encodeURIComponent(id)}/cancel`, {})); break;
      case 'again': runAgain(id); break;
      case 'invite': makeInvite(); break;
      case 'revoke': act(async () => { await api('DELETE', `/api/invites/${encodeURIComponent(id)}`); loadInvites(); }); break;
      case 'gh-save':
        act(async () => {
          const token = $('#ag-gh-token').value.trim();
          flash('#ag-gh-status', 'Checking the token with GitHub…');
          try {
            A.github = await api('POST', '/api/github/token', { token });
          } catch (err) { flash('#ag-gh-status', err.message, true); return; }
          $('#ag-gh-token').value = '';
          A.ghRepos = null;
          renderGitHub();
          loadGitHubRepos();
        });
        break;
      case 'gh-disconnect':
        if (confirm('Remove the GitHub token from the coordinator?')) act(async () => { A.github = await api('DELETE', '/api/github/token'); A.github.user = ''; A.ghRepos = null; renderGitHub(); });
        break;
      default: break;
    }
  }

  function onChange(e) {
    const card = e.target.closest('.ag-task');
    if (card) {
      if (e.target.dataset.f === 'adapter') e.target.dataset.chosen = '1';
      if (e.target.dataset.f === 'device') fillAdapterSelect(card);
      updateTaskCard(card);
    }
    if (e.target.id === 'ag-repo-kind') onRepoKind();
    if (e.target.id === 'ag-gh-repo') loadBranches();
  }

  function onInput(e) {
    if (e.target.id === 'ag-gh-repo' && /^[\w.-]+\/[\w.-]+$/.test(e.target.value.trim()) && (A.ghRepos || []).some((r) => r.name === e.target.value.trim())) loadBranches();
  }

  function renderAll() {
    renderRuns();
    renderComputers();
    renderGitHub();
    renderRepoChoices();
    onRepoKind();
    if (!A.github.connected && $('#ag-repo-kind').value === 'github' && !$('#ag-gh-repo').value) {
      $('#ag-repo-kind').value = 'local';
      onRepoKind();
    }
  }

  function onDevices(list) {
    A.devices = list || [];
    if (!$('#ag-body') || !$('#ag-comp-rows')) return;
    renderComputers();
    document.querySelectorAll('#ag-tasks .ag-task').forEach((card) => { fillDeviceSelect(card); fillAdapterSelect(card); updateTaskCard(card); });
  }

  window.addEventListener('yh:msg', (e) => {
    const m = e.detail || {};
    if (m.type === 'welcome' || m.type === 'devices') { onDevices(m.devices); return; }
    if (!A.loaded) return;
    if (m.type === 'agents.run' && m.run) {
      A.runs.set(m.run.id, m.run);
      renderRun(m.run.id);
    } else if (m.type === 'agents.task' && m.task) {
      const run = A.runs.get(m.task.runId);
      if (!run) return;
      const i = run.tasks.findIndex((t) => t.id === m.task.id);
      if (i >= 0) run.tasks[i] = m.task; else run.tasks.push(m.task);
      renderRun(run.id);
    } else if (m.type === 'agents.log' && A.viewer && A.viewer.mode === 'log' && A.viewer.taskId === m.task) {
      const pre = $('#ag-viewer-text');
      const atEnd = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 30;
      pre.textContent += m.data;
      if (pre.textContent.length > 400000) pre.textContent = pre.textContent.slice(-300000);
      if (atEnd) pre.scrollTop = pre.scrollHeight;
    }
  });

  let built = false;
  window.YHAgents = {
    opened() {
      if (!built) { build(); built = true; showTab(A.tab); if (window.YH && window.YH.devices) onDevices(window.YH.devices); }
      load();
    },
  };
})();
