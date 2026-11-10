'use strict';
// Home: one screen with what's running now and every connected device, what it shares, and
// what it's doing.
//
// Running now lists every batch job, test run, agent run and compute job in progress, with a
// bar, the devices working on it, and a button to its details. Devices shows one row each:
// whose it is, what it has, what it's doing, and, for the pool's owner, a switch to pause it
// (no new work of any kind; what runs finishes) and how many threads the pool may use on it.
// Phones and tablets take small tasks only (one thread) unless you give them more: they're
// mostly for watching.
(() => {
  const $ = (s, r = document) => r.querySelector(s);
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const nf = new Intl.NumberFormat();
  const H = { devices: [], me: null, caps: null, eng: new Map(), old: new Map(), runs: new Map(), loaded: false, loading: false,
    status: '', holding: false, dirty: false };
  const secs = (t) => (t > 1e11 ? t / 1000 : t); // compute jobs count in milliseconds, everything else in seconds
  const MOBILE = ['iPhone', 'iPad', 'Android'];

  function dur(from, to) {
    if (!from) return '';
    const s = Math.max(0, Math.round((to ? secs(to) : Date.now() / 1000) - secs(from)));
    if (s < 60) return `${s} s`;
    if (s < 3600) return `${Math.floor(s / 60)} min ${s % 60 ? `${s % 60} s` : ''}`.trim();
    return `${Math.floor(s / 3600)} h ${Math.round((s % 3600) / 60)} min`;
  }
  const ago = (t) => (t ? `${dur(t)} ago` : '');

  async function api(method, path, body) {
    const opts = { method, credentials: 'same-origin', headers: {} };
    if (body !== undefined) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
    const res = await fetch(path, opts);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `The pool answered ${res.status}.`);
    return data;
  }

  const isAdmin = () => Array.isArray(H.caps) && H.caps.includes('admin');
  const visible = () => { const w = $('#win-home'); return !!w && !w.hidden; };

  // ---------------------------------------------------------------- devices
  const limitsOf = (d) => d.limits || { paused: false, threads: d.threads, wantThreads: d.threads, wantSharing: d.sharing, cap: null,
    mobile: d.kind === 'browser' && MOBILE.includes((d.info || {}).platform) };

  function order(d) {
    if (d.kind === 'agent') return d.coordinator ? 0 : 1;
    return limitsOf(d).mobile ? 3 : 2;
  }
  const gb = (n) => (n == null ? null : nf.format(Math.round(n * 10) / 10));

  function what(d) {
    const p = (d.info || {}).platform;
    if (d.kind === 'agent') return p || 'Computer';
    if (p === 'iPhone' || p === 'iPad') return p;
    if (p === 'Android') return 'Android phone or tablet';
    return p ? `Browser tab, ${p}` : 'Browser tab';
  }

  function describe(d) {
    const i = d.info || {};
    const bits = [what(d)];
    if (i.cores) bits.push(`${nf.format(i.cores)} cores`);
    if (i.memoryGB) bits.push(i.memoryFreeGB != null ? `${gb(i.memoryFreeGB)} of ${gb(i.memoryGB)} GB memory free` : `${gb(i.memoryGB)} GB memory`);
    if (d.kind === 'agent' && i.diskFreeGB != null) bits.push(`${gb(i.diskFreeGB)} GB disk free`);
    if ((i.gpus || []).length) bits.push(i.gpus.join(', '));
    if (d.coordinator) bits.push('runs the pool');
    return bits.join(' · ');
  }

  function working(d) {
    const r = d.reserved || {};
    return (d.busy || 0) + (r.engine || 0) + (r.agentTasks || 0);
  }

  function stateOf(d) {
    const l = limitsOf(d);
    const blocked = ((d.engine || {}).profile || {}).blocked;
    if (d.stale) return ['stale', 'Not responding'];
    if (l.paused) return ['paused', 'Paused: takes no new work'];
    if (l.wantSharing === false) return ['off', 'Not sharing: turned off on that device'];
    const n = working(d);
    if (n) return ['working', `Working on ${nf.format(n)} piece${n === 1 ? '' : 's'}`];
    if (blocked && blocked !== 'sharing is off') return ['off', `Not taking work: ${blocked}`];
    return ['ready', l.mobile && l.cap == null ? 'Ready for small tasks' : 'Ready'];
  }

  function controls(d) {
    const l = limitsOf(d);
    const max = Math.max(1, l.wantThreads || d.threads || 1);
    if (!isAdmin()) return `<p class="hm-share">${l.paused ? 'Paused' : `Shares ${nf.format(d.threads)} of ${nf.format(max)} threads`}</p>`;
    const on = !l.paused;
    const fixed = !on || l.wantSharing === false;
    return `<div class="hm-ctrl">
      <div class="hm-step" role="group" aria-label="Threads the pool may use on ${esc(d.name)}">
        <button type="button" data-step="-1" data-id="${esc(d.id)}" aria-label="One thread fewer"${fixed || d.threads <= 1 ? ' disabled' : ''}>−</button>
        <span class="hm-step-n">${nf.format(d.threads)}</span>
        <button type="button" data-step="1" data-id="${esc(d.id)}" aria-label="One thread more"${fixed || d.threads >= max ? ' disabled' : ''}>+</button>
        <span class="hm-step-of">of ${nf.format(max)} threads</span>
      </div>
      <label class="hm-toggle"><input type="checkbox" data-share="${esc(d.id)}"${on ? ' checked' : ''}>
        <span class="switch-track" aria-hidden="true"></span><span class="sr-only">Let ${esc(d.name)} take work</span></label>
    </div>`;
  }

  function devRow(d) {
    const [state, text] = stateOf(d);
    const tags = (d.id === H.me ? '<span class="hm-tag">This device</span>' : '') + (d.owned === false ? '<span class="hm-tag">A friend’s</span>' : '');
    return `<li class="hm-card hm-dev" data-id="${esc(d.id)}" data-state="${state}">
      <span class="hm-dot" aria-hidden="true"></span>
      <div class="hm-dev-main">
        <p class="hm-dev-name"><b>${esc(d.name)}</b>${tags}</p>
        <p class="hm-dev-sub">${esc(describe(d))}</p>
        <p class="hm-dev-state">${esc(text)}</p>
      </div>
      ${controls(d)}
    </li>`;
  }

  // ---------------------------------------------------------------- what's running
  const WORKLOAD = {
    'batch.aggregate': 'Batch job', 'batch.groupby': 'Batch job', 'batch.transform': 'Batch job', 'batch.join': 'Batch job',
    'python.map': 'Your code', 'ml.evaluate': 'Model evaluation', 'tests.pytest': 'Test run',
  };
  const ENG_STATE = { running: 'Running', reducing: 'Combining the results', verifying: 'Checking the results', needs_approval: 'Waiting for your approval',
    done: 'Done', failed: 'Failed', cancelled: 'Cancelled' };
  const RUN_STATE = { queued: 'Waiting for a computer', preparing: 'Getting ready', running: 'Running', integrating: 'Combining the changes',
    verifying: 'Running the final checks', done: 'Done', failed: 'Failed', cancelled: 'Cancelled' };
  const FINISHED = ['done', 'failed', 'cancelled'];

  function engItem(j) {
    const done = (j.counts || {}).done || 0;
    return { key: `e:${j.id}`, app: 'batch', kind: WORKLOAD[j.workload] || 'Batch job', name: j.name, status: j.status, state: ENG_STATE[j.status] || j.status,
      at: j.startedAt || j.createdAt, end: j.finishedAt, pct: j.total ? done / j.total : 0, count: j.total ? `${nf.format(done)} of ${nf.format(j.total)} pieces` : '',
      who: (j.perDevice || []).map((p) => [p.name, p.n]) };
  }
  function oldItem(j) {
    return { key: `o:${j.id}`, app: 'jobs', kind: 'Compute job', name: j.title || j.kind, status: j.status, state: ENG_STATE[j.status] || j.status,
      at: j.startedAt || j.createdAt, end: j.finishedAt, pct: j.total ? (j.done || 0) / j.total : 0,
      count: j.total ? `${nf.format(j.done || 0)} of ${nf.format(j.total)} pieces` : '', who: Object.values(j.perDevice || {}).map((p) => [p.name, p.chunks]) };
  }
  function runItem(r) {
    const tasks = r.tasks || [];
    const done = tasks.filter((t) => FINISHED.includes(t.status)).length;
    const who = tasks.filter((t) => !FINISHED.includes(t.status)).map((t) => [t.deviceName || 'not started', t.phase || t.status]);
    return { key: `r:${r.id}`, app: 'agents', kind: 'Agent run', name: r.title, status: r.status, state: RUN_STATE[r.status] || r.status,
      at: r.createdAt, end: FINISHED.includes(r.status) ? r.updatedAt : null, pct: tasks.length ? done / tasks.length : 0,
      count: tasks.length ? `${nf.format(done)} of ${nf.format(tasks.length)} tasks done` : '', who, tasks: true };
  }

  function items() {
    return [...[...H.eng.values()].map(engItem), ...[...H.old.values()].map(oldItem), ...[...H.runs.values()].map(runItem)];
  }

  function liveRow(it) {
    const who = it.who.map(([name, n]) => `<span class="hm-chip"><b>${esc(name)}</b> ${esc(typeof n === 'number' ? nf.format(n) : n)}</span>`).join('');
    const wait = it.status === 'needs_approval' || it.status === 'queued';
    return `<li class="hm-card hm-job"${wait ? ' data-wait="true"' : ''}>
      <div class="hm-job-top">
        <p class="hm-job-title"><span class="hm-kind">${esc(it.kind)}</span> <b class="hm-job-name">${esc(it.name)}</b></p>
        <button class="btn ghost small" type="button" data-open="${it.app}">Open</button>
      </div>
      <div class="hm-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${Math.round(it.pct * 100)}"><span style="width:${(it.pct * 100).toFixed(1)}%"></span></div>
      <p class="hm-job-state">${esc(it.state)}${it.count ? ` · ${esc(it.count)}` : ''}${it.at ? ` · ${esc(dur(it.at))}` : ''}</p>
      ${who ? `<p class="hm-who">${it.tasks ? '' : '<span class="hm-who-h">Pieces done by</span>'}${who}</p>` : ''}
    </li>`;
  }

  function recentRow(it) {
    const ok = it.status === 'done';
    return `<li class="hm-recent-row"><span class="hm-mark" data-ok="${ok}">${ok ? '✓' : '×'}</span>
      <span class="hm-recent-name"><span class="hm-kind">${esc(it.kind)}</span> ${esc(it.name)}</span>
      <span class="hm-recent-when">${esc(it.state)} ${esc(ago(it.end || it.at))}</span>
      <button class="btn ghost small" type="button" data-open="${it.app}">Open</button></li>`;
  }

  // ---------------------------------------------------------------- the screen
  function render() {
    const box = $('#home');
    if (!box) return;
    if (H.holding) { H.dirty = true; return; } // a finger or mouse is down on a control: don't swap it out from under it
    H.dirty = false;
    const all = items();
    const live = all.filter((it) => !FINISHED.includes(it.status)).sort((a, b) => secs(b.at || 0) - secs(a.at || 0));
    const recent = all.filter((it) => FINISHED.includes(it.status) && Date.now() / 1000 - secs(it.end || it.at || 0) < 86400)
      .sort((a, b) => secs(b.end || b.at || 0) - secs(a.end || a.at || 0)).slice(0, 4);
    const devs = H.devices.slice().sort((a, b) => order(a) - order(b) || (b.id === H.me) - (a.id === H.me) || String(a.name).localeCompare(String(b.name)));
    const sharing = devs.filter((d) => !d.stale && d.sharing);
    const threads = sharing.reduce((n, d) => n + (d.threads || 0), 0);
    const busy = devs.filter((d) => working(d) > 0).length;
    const computers = devs.filter((d) => d.kind === 'agent');
    const others = devs.filter((d) => d.kind !== 'agent');
    const stat = (n, one, many) => `<span class="hm-stat"><b>${nf.format(n)}</b> ${n === 1 ? one : many}</span>`;
    box.innerHTML = `
      <div class="hm-head">
        <p class="hm-sum">${stat(devs.length, 'device connected', 'devices connected')}${stat(threads, 'thread shared', 'threads shared')}${
          stat(busy, 'working', 'working')}${stat(live.length, 'job running', 'jobs running')}</p>
        <button class="btn ghost small" type="button" data-open="devices">Add a device</button>
      </div>
      ${H.status ? `<p class="hm-status error" role="alert">${esc(H.status)}</p>` : ''}
      <section class="hm-sec" aria-labelledby="hm-run-h">
        <h2 id="hm-run-h">Running now</h2>
        ${live.length ? `<ul class="hm-list">${live.map(liveRow).join('')}</ul>`
          : `<p class="hm-empty">Nothing is running. Start work from <button class="linkish" type="button" data-open="batch">Batch</button>,
             <button class="linkish" type="button" data-open="agents">Agents</button> or <code>harness test</code>.</p>`}
        ${recent.length ? `<h3 class="hm-sub-h">Finished recently</h3><ul class="hm-recent">${recent.map(recentRow).join('')}</ul>` : ''}
      </section>
      <section class="hm-sec" aria-labelledby="hm-dev-h">
        <h2 id="hm-dev-h">Devices</h2>
        ${isAdmin() ? '<p class="hint hm-help">Switching a device off pauses it: it takes no new work, and what it’s running finishes. The number is how many of its threads the pool may use.</p>' : ''}
        ${computers.length ? `<h3 class="hm-sub-h">Computers</h3><ul class="hm-list">${computers.map(devRow).join('')}</ul>` : ''}
        ${others.length ? `<h3 class="hm-sub-h">Phones, tablets and browser tabs</h3><ul class="hm-list">${others.map(devRow).join('')}</ul>` : ''}
        ${devs.length ? '' : '<p class="hm-empty">No devices are connected.</p>'}
      </section>`;
    const sub = $('#wsub-home');
    if (sub) sub.textContent = live.length ? `${live.length} running` : '';
  }

  let frame = 0;
  function schedule() {
    if (frame) return;
    frame = requestAnimationFrame(() => { frame = 0; render(); });
  }

  async function setLimits(id, body) {
    const d = H.devices.find((x) => x.id === id);
    if (!d) return;
    const before = JSON.parse(JSON.stringify(d));
    const l = d.limits = { ...limitsOf(d) };
    if ('paused' in body) { l.paused = body.paused; d.sharing = !body.paused && l.wantSharing !== false; }
    if ('threads' in body) { l.cap = body.threads; d.threads = body.threads ?? (l.mobile ? 1 : l.wantThreads || d.threads); }
    H.status = '';
    render();
    try {
      const r = await api('POST', `/api/devices/${encodeURIComponent(id)}/limits`, body);
      if (r.device) Object.assign(d, r.device);
    } catch (e) {
      Object.assign(d, before);
      H.status = `${before.name}: ${e.message}`;
    }
    render();
  }

  async function load() {
    if (H.loading) return;
    H.loading = true;
    try {
      const missed = H.caps == null; // this script loaded after the welcome message: ask for what it carried
      const [eng, agents, old, me, devs] = await Promise.allSettled([api('GET', '/api/engine'), api('GET', '/api/agents'), api('GET', '/api/jobs'),
        missed ? api('GET', '/api/me') : null, missed ? api('GET', '/api/devices') : null]);
      if (eng.status === 'fulfilled') (eng.value.jobs || []).forEach((j) => H.eng.set(j.id, j));
      if (agents.status === 'fulfilled') (agents.value.runs || []).forEach((r) => H.runs.set(r.id, r));
      if (old.status === 'fulfilled') (old.value.jobs || []).forEach((j) => H.old.set(j.id, j));
      if (missed && me.status === 'fulfilled' && H.caps == null) { H.me = me.value.deviceId; H.caps = me.value.caps || []; }
      if (missed && devs.status === 'fulfilled' && !H.devices.length) H.devices = devs.value.devices || [];
      H.loaded = true;
    } finally {
      H.loading = false;
    }
    render();
  }

  function bind() {
    const box = $('#home');
    if (!box || box.dataset.bound) return;
    box.dataset.bound = '1';
    box.addEventListener('pointerdown', (e) => { if (e.target.closest('button, input, label')) H.holding = true; });
    const release = () => { if (!H.holding) return; H.holding = false; if (H.dirty) setTimeout(render, 0); };
    window.addEventListener('pointerup', release);
    window.addEventListener('pointercancel', release);
    box.addEventListener('click', (e) => {
      const b = e.target.closest('button[data-step]');
      if (!b || b.disabled) return;
      const d = H.devices.find((x) => x.id === b.dataset.id);
      if (!d) return;
      const l = limitsOf(d);
      const max = Math.max(1, l.wantThreads || d.threads || 1);
      const n = Math.max(1, Math.min(max, (d.threads || 1) + Number(b.dataset.step)));
      // A computer back at every thread it offers has no limit, so it gets more if it offers more
      // later. A phone or tablet keeps the number: no limit there means small tasks only.
      if (n !== d.threads) setLimits(d.id, { threads: n >= max && !l.mobile ? null : n });
    });
    box.addEventListener('change', (e) => {
      const c = e.target.closest('input[data-share]');
      if (c) setLimits(c.dataset.share, { paused: !c.checked });
    });
  }

  window.addEventListener('yh:msg', (e) => {
    const m = e.detail || {};
    if (m.type === 'welcome') {
      H.me = m.id;
      H.caps = m.caps || [];
      H.devices = m.devices || [];
      (m.jobs || []).forEach((j) => H.old.set(j.id, j));
    } else if (m.type === 'devices') {
      H.devices = m.devices || [];
    } else if (m.type === 'job' && m.job) {
      H.old.set(m.job.id, m.job);
    } else if (m.type === 'progress') {
      const j = H.old.get(m.jobId);
      if (!j) return;
      j.done = m.done;
      const pd = (j.perDevice = j.perDevice || {})[m.deviceId] || (j.perDevice[m.deviceId] = { name: m.deviceName, chunks: 0 });
      pd.chunks += 1;
    } else if (m.type === 'eng.job' && m.job) {
      if (m.job.deleted) H.eng.delete(m.job.id);
      else H.eng.set(m.job.id, { ...(H.eng.get(m.job.id) || {}), ...m.job });
    } else if (m.type === 'agents.run' && m.run) {
      H.runs.set(m.run.id, m.run);
    } else if (m.type === 'agents.task' && m.task) {
      const r = H.runs.get(m.task.runId);
      if (!r) return;
      const i = (r.tasks = r.tasks || []).findIndex((t) => t.id === m.task.id);
      if (i >= 0) r.tasks[i] = m.task; else r.tasks.push(m.task);
    } else {
      return;
    }
    if (visible()) schedule();
  });

  setInterval(() => { // the running times count up
    if (visible() && !H.holding && items().some((it) => !FINISHED.includes(it.status))) render();
  }, 1000);

  window.YHHome = {
    opened() { bind(); render(); load(); },
    render,
  };
})();
