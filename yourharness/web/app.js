'use strict';
(() => {
  // -------------------------------------------------------------------------
  // Basics
  // -------------------------------------------------------------------------
  const $ = (sel, el = document) => el.querySelector(sel);
  const nf = new Intl.NumberFormat();
  const store = {
    get: (k) => { try { return localStorage.getItem(`yh.${k}`); } catch { return null; } },
    set: (k, v) => { try { localStorage.setItem(`yh.${k}`, String(v)); } catch { /* private mode */ } },
    del: (k) => { try { localStorage.removeItem(`yh.${k}`); } catch { /* private mode */ } },
  };
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const newId = () => Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
  const reduceMotion = () => matchMedia('(prefers-reduced-motion: reduce)').matches;

  const NATIVE_APP = new URLSearchParams(location.search).has('native');
  const CORES = Math.max(1, navigator.hardwareConcurrency || 4);
  const MAX_THREADS = Math.min(32, CORES);
  const PLATFORM = detectPlatform();
  const BEAT_MS = 4000;

  const FRACTAL_PLACES = {
    elephant: { cx: 0.285, cy: 0.0115, span: 0.012 },
    spiral: { cx: -0.761574, cy: -0.0847596, span: 0.0035 },
    seahorse: { cx: -0.7453, cy: 0.1127, span: 0.0065 },
    whole: { cx: -0.6, cy: 0, span: 3.2 },
  };

  const DEFAULT_CODE = {
    js: `// Runs once per piece, in a background thread on every sharing browser.
// chunk goes from 0 to total - 1. Return a number, or anything JSON can hold.
function run(chunk, total) {
  const n = 2e9;
  let sum = 0;
  for (let i = chunk; i < n; i += total) sum += Math.sqrt(i);
  return sum;
}
`,
    python: `# Runs once per piece, in a worker process on every agent (computers).
# chunk goes from 0 to total - 1. Return a number, or anything JSON can hold.
def run(chunk, total):
    import math
    n = 50_000_000
    return sum(math.sqrt(i) for i in range(chunk, n, total))
`,
  };

  const S = {
    name: store.get('name') || defaultName(),
    threads: clampThreads(Number(store.get('threads')) || Math.max(1, CORES - 1)),
    sharing: NATIVE_APP ? store.get('sharing') === 'on' : store.get('sharing') !== 'off', // the desktop app shares through its own agent
    id: null,
    color: null,
    joinToken: null,
    lendToken: null,
    caps: [],
    devices: [],
    jobs: [],
    files: [],
    pool: { files: [], agents: 0, storedBytes: 0, freeGB: 0, replicas: 2 },
    view: null, // { jobId, colors: [], values: [] | null }
    follow: true,
    ws: null,
    retry: 0,
    joined: false,
    replaced: false,
    deniedReason: '',
  };
  const jobParams = new Map(); // job id -> params, sent once per job by the coordinator

  function detectPlatform() {
    const ua = navigator.userAgent;
    const p = (navigator.userAgentData && navigator.userAgentData.platform) || navigator.platform || '';
    if (/iPhone/.test(ua)) return 'iPhone';
    if (/iPad/.test(ua) || (/Macintosh/.test(ua) && navigator.maxTouchPoints > 1)) return 'iPad';
    if (/Android/.test(ua)) return 'Android';
    if (/Mac/i.test(p) || /Macintosh/.test(ua)) return 'Mac';
    if (/Win/i.test(p) || /Windows/.test(ua)) return 'Windows';
    if (/Linux/i.test(p)) return 'Linux';
    return 'Device';
  }
  function defaultName() {
    return { Windows: 'Windows PC', Linux: 'Linux machine', Device: 'This device' }[PLATFORM] || PLATFORM;
  }
  function clampThreads(n) {
    return Math.max(1, Math.min(MAX_THREADS, Math.floor(n) || 1));
  }
  async function deviceInfo() {
    let storageGB = null;
    try {
      const est = await navigator.storage.estimate(); // browser storage this site could use; Safari reports it too
      if (est && est.quota) storageGB = Math.round(((est.quota - (est.usage || 0)) / 2 ** 30) * 10) / 10;
    } catch { /* not available */ }
    return {
      platform: PLATFORM,
      cores: CORES,
      memoryGB: typeof navigator.deviceMemory === 'number' ? navigator.deviceMemory : null, // Safari doesn't report it
      storageGB,
      gpus: 'gpu' in navigator ? ['WebGPU'] : [],
    };
  }

  // -------------------------------------------------------------------------
  // Signing in. The coordinator sets an HttpOnly cookie, so nothing secret is
  // kept in this page or in any URL after the join link is used once.
  // -------------------------------------------------------------------------
  function parseJoin(text) {
    const t = String(text || '').trim();
    const m = /[#?&]join=([A-Za-z0-9_-]+)/.exec(t);
    if (m) return m[1];
    return /^\d{4}$/.test(t) || /^[A-Za-z0-9_-]{16,}$/.test(t) ? t : null;
  }

  // What this device is allowed to do. A computer that joined with a lend link
  // computes pieces and nothing else, so the apps it can't use are hidden rather
  // than left to fail: the coordinator refuses them anyway, and a button that
  // always errors is worse than no button.
  function can(cap) { return !S.caps || S.caps.indexOf(cap) >= 0; }

  function applyCaps() {
    const gated = { terminal: 'shell', files: 'files' };
    Object.keys(gated).forEach(function (app) {
      const ok = can(gated[app]);
      document.querySelectorAll('[data-open="' + app + '"]').forEach(function (b) { b.hidden = !ok; });
    });
    const lending = !can('submit') && !can('shell') && !can('files');
    document.body.classList.toggle('lending', lending);
    const note = document.getElementById('lend-note');
    if (note) note.hidden = !lending;
  }

  async function joinWith(token, name) {
    const res = await fetch('/api/join', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token, name }),
      credentials: 'same-origin',
    });
    if (!res.ok) {
      let msg = `The coordinator answered ${res.status}.`;
      try { msg = (await res.json()).error || msg; } catch { /* not JSON */ }
      throw new Error(msg);
    }
    return res.json();
  }

  // -------------------------------------------------------------------------
  // Connection
  // -------------------------------------------------------------------------
  function send(msg) {
    if (S.ws && S.ws.readyState === 1) S.ws.send(JSON.stringify(msg));
  }

  function connect() {
    if (S.ws && S.ws.readyState <= 1) return;
    S.replaced = false;
    $('#replaced').hidden = true;
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${proto}//${location.host}/ws`);
    S.ws = ws;
    setConn('connecting');
    ws.onopen = async () => {
      const info = await deviceInfo();
      if (ws.readyState === 1) {
        ws.send(JSON.stringify({ type: 'hello', name: S.name, threads: S.threads, sharing: S.sharing, info, runtimes: ['js'] }));
      }
    };
    ws.onmessage = (e) => {
      let m;
      try { m = JSON.parse(e.data); } catch { return; }
      handle(m);
    };
    ws.onclose = (e) => {
      if (S.ws !== ws) return;
      S.ws = null;
      setConn('offline');
      if (e.code === 4401) { signedOut(S.deniedReason); return; }
      if (e.code === 4002) { // the same device connected from another tab
        S.replaced = true;
        $('#replaced').hidden = false;
        dropAllWork();
        return;
      }
      const delay = Math.min(5000, 400 * 2 ** S.retry++);
      setTimeout(() => { if (!S.ws && !S.replaced && S.joined) connect(); }, delay);
    };
  }

  // Tell the coordinator this page is alive. A page the browser has frozen stops
  // sending these, and its pieces move to other devices within seconds.
  setInterval(() => send({ type: 'ping', t: performance.now() }), BEAT_MS);

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible' && S.joined && !S.replaced && !S.ws) {
      S.retry = 0;
      connect();
    }
    updateWakeLock();
  });

  function handle(m) {
    switch (m.type) {
      case 'welcome': {
        const termWasOpen = T.open && !!S.id; // a reconnect: reopen the terminal that was open
        S.retry = 0;
        S.id = m.id;
        S.color = m.color;
        S.joinToken = m.joinToken;
        S.joinCode = m.joinCode;
        S.lendToken = m.lendToken;
        S.caps = m.caps || [];
        applyCaps();
        S.procs = m.procs || [];
        S.address = m.address || location.origin;
        S.joined = true;
        S.devices = m.devices;
        S.jobs = m.jobs;
        S.files = m.files || [];
        if (m.pool) S.pool = m.pool;
        if (m.name && m.name !== S.name) { S.name = m.name; store.set('name', S.name); }
        showApp();
        setConn('online');
        if ((S.follow || !S.view) && m.snapshot) setView(m.snapshot);
        renderAll();
        if (termWasOpen) openTerminal(T.kind, T.hostId);
        else if (WM.isOpen('terminal')) autoOpenTerminal();
        send({ type: 'ping', t: performance.now() });
        break;
      }
      case 'denied':
        S.deniedReason = m.reason || '';
        break;
      case 'devices':
        S.devices = m.devices;
        renderDevices();
        renderWorkspace();
        fmRenderLocations();
        if (WM.isOpen('terminal')) autoOpenTerminal();
        break;
      case 'pong':
        if (typeof m.t === 'number') { S.rtt = Math.round(performance.now() - m.t); renderTopbar(); }
        break;
      case 'fs.changed':
        if (FM.loc && FM.loc.type === 'fs' && FM.loc.device === m.device && FM.path === (m.path || '')) fmLoad();
        break;
      case 'job':
        upsertJob(m.job);
        break;
      case 'progress':
        onProgress(m);
        break;
      case 'snapshot':
        setView(m.snapshot);
        renderCurrent(true);
        renderHistory();
        break;
      case 'chunk': {
        if (m.params) jobParams.set(m.jobId, m.params);
        const params = jobParams.get(m.jobId);
        if (!params) { send({ type: 'failed', jobId: m.jobId, idx: m.idx, error: 'This device missed the job settings.' }); break; }
        pool.queue.push({ ...m, params });
        pump();
        break;
      }
      case 'abort':
        dropJob(m.jobId);
        break;
      case 'files':
        S.files = m.files;
        renderFiles();
        if (FM.loc && FM.loc.type === 'shared') fmRender();
        break;
      case 'pool':
        S.pool = m.pool;
        fmRenderLocations();
        if (FM.loc && FM.loc.type === 'os') fmLoad(true);
        break;
      case 'error':
        $('#run-error').textContent = m.message;
        break;
      case 'term.opened':
      case 'term.out':
      case 'term.exit':
      case 'term.error':
      case 'os.opened':
      case 'os.out':
      case 'os.exit':
        onTerminal(m);
        break;
      case 'procs':
        S.procs = m.procs;
        renderProcs();
        break;
      case 'joinCode':
        S.joinCode = m.joinCode;
        renderAdd();
        renderTopbar();
        break;
      default:
        break;
    }
  }

  function setConn(state) {
    S.conn = state;
    $('#conn').dataset.state = state;
    renderTopbar();
  }

  // -------------------------------------------------------------------------
  // Local worker pool: one Web Worker per shared thread
  // -------------------------------------------------------------------------
  const pool = { slots: [], queue: [], dropped: new Set() };

  function makeSlot() {
    const slot = { w: new Worker('worker.js', { type: 'module' }), task: null, retire: false };
    slot.w.onmessage = (e) => finishSlot(slot, e.data);
    slot.w.onerror = (e) => {
      e.preventDefault();
      finishSlot(slot, { ok: false, error: e.message || 'The background thread stopped.', ms: 0 });
    };
    return slot;
  }
  function resizePool(n) {
    let active = pool.slots.filter((s) => !s.retire);
    while (active.length < n) { pool.slots.push(makeSlot()); active = pool.slots.filter((s) => !s.retire); }
    for (let i = active.length - 1; i >= n; i--) {
      const s = active[i];
      if (s.task) s.retire = true;
      else { s.w.terminate(); pool.slots.splice(pool.slots.indexOf(s), 1); }
    }
    pump();
  }
  function pump() {
    for (const s of pool.slots) {
      if (s.retire || s.task) continue;
      const t = pool.queue.shift();
      if (!t) break;
      s.task = t;
      s.w.postMessage({ task: t });
    }
  }
  function finishSlot(slot, data) {
    const t = slot.task;
    slot.task = null;
    if (t && !pool.dropped.has(t.jobId)) {
      if (data.ok) send({ type: 'result', jobId: t.jobId, idx: t.idx, value: data.value, ms: data.ms });
      else send({ type: 'failed', jobId: t.jobId, idx: t.idx, error: data.error });
    }
    if (slot.retire) {
      slot.w.terminate();
      pool.slots.splice(pool.slots.indexOf(slot), 1);
    }
    pump();
  }
  function dropJob(jobId) {
    pool.dropped.add(jobId);
    jobParams.delete(jobId);
    pool.queue = pool.queue.filter((t) => t.jobId !== jobId);
    pool.slots.forEach((s, i) => {
      if (!s.task || s.task.jobId !== jobId) return;
      s.w.terminate(); // stops long-running pieces right away
      pool.slots[i] = s.retire ? null : makeSlot();
    });
    pool.slots = pool.slots.filter(Boolean);
    pump();
  }
  function dropAllWork() {
    pool.queue = [];
    resizePool(0);
  }

  // -------------------------------------------------------------------------
  // Keep the screen on while sharing (needs HTTPS or localhost)
  // -------------------------------------------------------------------------
  let wakeLock = null;
  async function updateWakeLock() {
    const want = S.joined && S.sharing && document.visibilityState === 'visible';
    if ('wakeLock' in navigator) {
      try {
        if (want && !wakeLock) {
          wakeLock = await navigator.wakeLock.request('screen');
          wakeLock.addEventListener('release', () => { wakeLock = null; renderAwake(); });
        } else if (!want && wakeLock) {
          await wakeLock.release();
          wakeLock = null;
        }
      } catch { wakeLock = null; }
    }
    renderAwake();
  }
  function renderAwake() {
    const el = $('#me-awake');
    if (!S.sharing) el.textContent = 'This device is connected but not taking any pieces.';
    else if (wakeLock) el.textContent = 'The screen stays on while this device is sharing.';
    else if (['iPhone', 'iPad', 'Android'].includes(PLATFORM)) el.textContent = 'Keep this page open and the screen on. Phones and tablets pause pages in the background, and their pieces move to other devices.';
    else el.textContent = 'Keep this tab open. Background tabs can be slowed down by the browser.';
  }

  // -------------------------------------------------------------------------
  // Workspace terminal
  // -------------------------------------------------------------------------
  // T.kind is 'os' (the YourHarness OS shell: commands go to whichever computer is least busy)
  // or 'direct' (a shell that lives on one chosen computer).
  const T = { term: null, fit: null, sid: null, kind: 'os', hostId: null, ref: null, open: false, loading: null };
  const KEYS = { esc: '\x1b', tab: '\t', ctrlc: '\x03', ctrld: '\x04', up: '\x1b[A', down: '\x1b[B', right: '\x1b[C', left: '\x1b[D' };
  const LINUX = /ubuntu|linux|debian|fedora/i;

  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const el = document.createElement('script');
      el.src = src;
      el.onload = resolve;
      el.onerror = () => reject(new Error(`Could not load ${src}`));
      document.head.appendChild(el);
    });
  }
  const termMsg = (suffix, extra) => ({ type: `${T.kind === 'os' ? 'os' : 'term'}.${suffix}`, sid: T.sid, ...extra });
  function ensureTerminal() {
    if (T.term) return Promise.resolve();
    if (!T.loading) {
      T.loading = loadScript('vendor/xterm.js').then(() => loadScript('vendor/addon-fit.js')).then(() => {
        T.term = new window.Terminal({
          cursorBlink: true,
          fontFamily: "ui-monospace, 'SF Mono', Menlo, Consolas, 'DejaVu Sans Mono', monospace",
          fontSize: matchMedia('(max-width: 480px)').matches ? 12 : 14,
          scrollback: 5000,
          theme: { background: '#0F141A', foreground: '#E6EBF0', cursor: '#E0A100', selectionBackground: '#2F3A47' },
        });
        T.fit = new window.FitAddon.FitAddon();
        T.term.loadAddon(T.fit);
        T.term.open($('#term'));
        T.term.onData((d) => { if (T.sid) send(termMsg('in', { data: d })); });
        T.term.onResize(({ cols, rows }) => { if (T.sid) send(termMsg('resize', { cols, rows })); });
      });
    }
    return T.loading;
  }

  // Computers that can run the OS shell's commands, and computers that can host a direct shell.
  const runners = () => S.devices.filter((d) => d.exec && !d.stale && d.sharing);
  const directHosts = () => S.devices.filter((d) => d.workspace && !d.stale);
  const selectedHost = () => directHosts().find((h) => h.id === T.hostId) || directHosts().find((h) => LINUX.test(h.workspace.system)) || directHosts()[0] || null;
  const osAvailable = () => runners().length > 0;

  function autoOpenTerminal() {
    if (T.open || T.autoDone) return;
    if (osAvailable()) { T.autoDone = true; openTerminal('os'); }
    else if (selectedHost()) { T.autoDone = true; openTerminal('direct', selectedHost().id); }
  }
  async function openTerminal(kind = T.kind, hostId = T.hostId) {
    if (T.sid) send(termMsg('close'));
    T.sid = null;
    T.kind = kind === 'direct' ? 'direct' : 'os';
    const host = T.kind === 'direct' ? (directHosts().find((h) => h.id === hostId) || selectedHost()) : null;
    if (T.kind === 'direct' && !host) { renderWorkspace(); return; }
    T.hostId = host ? host.id : T.hostId;
    T.open = true;
    renderWorkspace();
    try {
      await ensureTerminal();
    } catch (err) {
      $('#ws-status').textContent = err.message;
      return;
    }
    fitTerminal();
    T.ref = newId();
    T.term.reset();
    if (T.kind === 'direct') {
      T.term.write(`\x1b[2mConnecting to ${host.name}…\x1b[0m\r\n`);
      send({ type: 'term.open', cols: T.term.cols, rows: T.term.rows, ref: T.ref, host: host.id });
    } else {
      send({ type: 'os.open', cols: T.term.cols, rows: T.term.rows, ref: T.ref });
    }
  }
  function closeTerminal() {
    if (T.sid) send(termMsg('close'));
    T.sid = null;
    T.open = false;
    renderWorkspace();
  }
  function fitTerminal() {
    if (!T.fit || !T.open) return;
    try { T.fit.fit(); } catch { /* not visible yet */ }
  }
  function b64bytes(b64) {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  function onTerminal(m) {
    if (!T.term) return;
    const os = m.type.startsWith('os.');
    if (os !== (T.kind === 'os')) return;
    const kind = m.type.split('.')[1];
    if (kind === 'opened' && m.ref === T.ref) {
      T.sid = m.sid;
      if (!os) {
        T.hostId = m.hostId;
        const ws = m.workspace || {};
        T.term.write(`\x1b[1;33mThis shell runs on ${m.host}\x1b[0m\x1b[2m · ${ws.system || ''} · ${ws.shell || ''}. Everything you run here uses that computer.\x1b[0m\r\n`);
      }
      renderWorkspace();
      fitTerminal();
      send(termMsg('resize', { cols: T.term.cols, rows: T.term.rows }));
      T.term.focus();
    } else if (kind === 'error' && m.ref === T.ref) {
      T.term.write(`\r\n${m.message}\r\n`);
    } else if (kind === 'out' && m.sid === T.sid) {
      T.term.write(b64bytes(m.data));
    } else if (kind === 'exit' && m.sid === T.sid) {
      T.sid = null;
      T.term.write(`\r\n\x1b[2m${m.message || 'The shell ended.'} Press Reconnect to start again.\x1b[0m\r\n`);
      renderWorkspace();
    }
  }

  function renderWorkspace() {
    const rs = runners();
    const hosts = directHosts();
    const host = selectedHost();
    const mine = myHost();
    const isOs = T.kind === 'os';
    const linux = rs.filter((r) => LINUX.test(r.exec.os));
    const using = linux.length ? linux : rs;
    $('#ws-hosts').innerHTML = `<button class="host-tab os-tab" type="button" role="tab" data-kind="os" aria-selected="${isOs}" style="--wire:var(--ink)">
        <b>YourHarness OS</b><span>${new Set(rs.map((r) => r.host || r.id)).size > 1 ? `one shell for all ${new Set(rs.map((r) => r.host || r.id)).size} computers` : 'one shell for the pool'} · files in /pool</span></button>`
      + hosts.map((h) => `<button class="host-tab" type="button" role="tab" data-host="${esc(h.id)}" aria-selected="${!isOs && !!host && h.id === host.id}" style="--wire:${esc(h.color)}">
        <b>${esc(h.name)}${h.host && h.host === mine ? ' (this computer)' : ''}</b><span>direct shell · ${esc(h.workspace.system)}</span></button>`).join('');

    const where = $('#ws-where');
    if (isOs) {
      where.style.setProperty('--wire', 'var(--ink)');
      if (!rs.length) {
        where.innerHTML = `<div class="where-main">No computer can run commands yet</div>
          <p>Open the YourHarness app on a Mac or Windows PC and keep sharing on. Phones and tablets can't run commands.</p>`;
      } else {
        const rows = using.map((r) => {
          const cpu = r.load && typeof r.load.cpu === 'number' ? `${Math.round(r.load.cpu)}% CPU` : '';
          const busy = S.procs.some((p) => p.status === 'running' && p.runner === r.name);
          return `<span class="runner" style="--wire:${esc(r.color)}"><b>${esc(r.name)}</b> ${esc(r.exec.os)} · ${busy ? 'running a command' : 'idle'}${cpu ? ` · ${cpu}` : ''}</span>`;
        }).join('');
        where.innerHTML = `<div class="where-main">Runs on the least-busy computer</div>
          <div class="runners">${rows}</div>
          ${linux.length ? '' : '<p class="where-note">No Linux computer is connected, so commands run on macOS for now.</p>'}
          <details><summary>How this works</summary>
            <p>This shell belongs to the pool, not to a computer. Each command goes to whichever of these computers is free, your files in <code>/pool</code> go with it, and the line after each command says where it ran.</p>
            <p>Try <code>par echo {} ::: a b c d</code> to spread work over every computer, or <code>on &lt;computer&gt; &lt;command&gt;</code> to pick one. Commands can't take keyboard input; for an editor or other interactive tool, pick a direct shell.</p>
          </details>`;
      }
      $('#term-title').textContent = 'YourHarness OS';
      $('#term-title').style.setProperty('--wire', 'var(--ink)');
    } else if (!host) {
      where.style.removeProperty('--wire');
      where.innerHTML = '<div class="where-main">No computer can host a terminal right now</div><p>Open the YourHarness app on a Mac or Windows PC.</p>';
    } else {
      const ws = host.workspace;
      const remote = !(host.host && host.host === mine);
      where.style.setProperty('--wire', host.color);
      where.innerHTML = `<div class="where-main">Direct shell: runs only on ${esc(host.name)}</div>
        <p>${esc(ws.system)}, ${esc(ws.shell)} shell. Commands here use ${esc(host.name)}'s own processor, memory and files and are <b>not</b> shared with the pool${remote ? `, and not ${esc(myMachineName())}'s` : ''}. Use the YourHarness OS tab for pooled work.</p>`;
      $('#term-title').textContent = `${host.name} — ${ws.system} (direct)`;
      $('#term-title').style.setProperty('--wire', host.color);
    }
    const live = T.open && !!T.sid && (isOs || (host && T.hostId === host.id));
    $('#ws-open').hidden = live;
    $('#ws-open').disabled = isOs ? !rs.length : !host;
    $('#ws-open').textContent = T.open ? 'Reconnect' : isOs ? 'Open YourHarness OS' : `Open shell on ${host ? host.name : ''}`;
    $('#ws-close').hidden = !T.open;
    $('#term-wrap').hidden = !T.open;
    $('#wsub-terminal').textContent = isOs ? 'YourHarness OS · runs on the least-busy computer' : (host ? `direct shell on ${host.name}` : '');
    requestAnimationFrame(fitTerminal);
    $('#ws-status').textContent = '';
  }

  // -------------------------------------------------------------------------
  // Jobs and the current view
  // -------------------------------------------------------------------------
  const jobById = (id) => S.jobs.find((j) => j.id === id);
  const viewJob = () => (S.view ? jobById(S.view.jobId) : null);

  function emptyView(job) {
    return {
      jobId: job.id,
      colors: new Array(job.total).fill(null),
      values: job.kind === 'mandelbrot' ? new Array(job.total).fill(null) : null,
    };
  }

  function setView(snap) {
    const i = S.jobs.findIndex((j) => j.id === snap.job.id);
    if (i >= 0) S.jobs[i] = snap.job; else S.jobs.push(snap.job);
    S.view = {
      jobId: snap.job.id,
      colors: snap.colors || new Array(snap.job.total).fill(null),
      values: snap.job.kind === 'mandelbrot' ? (snap.values || new Array(snap.job.total).fill(null)) : null,
    };
  }

  function upsertJob(job) {
    const i = S.jobs.findIndex((j) => j.id === job.id);
    if (i >= 0) S.jobs[i] = job; else S.jobs.push(job);
    if (S.jobs.length > 20) S.jobs.splice(0, S.jobs.length - 20);
    if (job.status !== 'running' && job.status !== 'queued') {
      pool.queue = pool.queue.filter((t) => t.jobId !== job.id);
      jobParams.delete(job.id);
    }
    let switched = false;
    if (!S.view || (S.follow && job.status === 'running' && S.view.jobId !== job.id)) {
      S.view = emptyView(job);
      switched = true;
    }
    renderCurrent(switched);
    renderHistory();
    renderDevices();
  }

  function onProgress(m) {
    const job = jobById(m.jobId);
    if (job) {
      job.done = m.done;
      const pd = (job.perDevice[m.deviceId] ||= { name: m.deviceName, color: m.color, chunks: 0, ms: 0 });
      pd.chunks += 1;
      pd.ms += m.ms;
    }
    if (S.view && S.view.jobId === m.jobId) {
      S.view.colors[m.idx] = m.color;
      if (S.view.values && m.value) {
        S.view.values[m.idx] = m.value;
        drawBand(m.idx);
      }
      drawCell(m.idx);
      scheduleText();
    }
  }

  let textQueued = false;
  function scheduleText() {
    if (textQueued) return;
    textQueued = true;
    requestAnimationFrame(() => {
      textQueued = false;
      renderCurrent(false);
      renderDevices();
    });
  }

  // Elapsed time ticks while a run is going
  setInterval(() => {
    const j = viewJob();
    if (j && j.status === 'running') $('#cur-status').textContent = statusText(j);
  }, 250);

  // -------------------------------------------------------------------------
  // Rendering: devices
  // -------------------------------------------------------------------------
  const gb = (n) => nf.format(Math.round(n * 10) / 10);

  const myDevice = () => S.devices.find((d) => d.id === S.id);
  const myHost = () => (myDevice() || {}).host || null;
  const naText = (t) => `<span class="res-na">${t}</span>`;

  // Group connections into physical machines: a computer running the app and a browser tab is one machine.
  function machines() {
    const groups = new Map();
    for (const d of S.devices) {
      const key = d.host || d.id;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(d);
    }
    return [...groups.values()].map((list) => {
      const agent = list.find((d) => d.kind === 'agent');
      const main = agent || list[0];
      const live = list.filter((d) => d.sharing && !d.stale);
      return {
        key: main.host || main.id, list, main, agent,
        here: list.some((d) => d.id === S.id) || (!!main.host && main.host === myHost()),
        coordinator: list.some((d) => d.coordinator),
        terminal: list.find((d) => d.workspace),
        stale: list.every((d) => d.stale),
        sharing: live.length > 0,
        threads: Math.min(main.info.cores || Infinity, live.reduce((a, d) => a + d.threads, 0)),
        busy: list.reduce((a, d) => a + d.busy, 0),
        done: list.reduce((a, d) => a + d.done, 0),
      };
    }).sort((a, b) => (b.here - a.here) || (b.coordinator - a.coordinator) || a.main.joinedAt - b.main.joinedAt);
  }

  function renderDevices() {
    const ms = machines();
    const sharing = ms.filter((m) => m.sharing);
    const threads = sharing.reduce((a, m) => a + m.threads, 0);
    const cores = sharing.reduce((a, m) => a + m.main.info.cores, 0);
    const memMs = sharing.filter((m) => m.main.info.memoryGB);
    const mem = memMs.reduce((a, m) => a + m.main.info.memoryGB, 0);
    const memFree = memMs.reduce((a, m) => a + (m.main.info.memoryFreeGB || 0), 0);
    const disk = sharing.reduce((a, m) => a + (m.agent && m.agent.info.diskFreeGB || 0), 0);
    const gpus = sharing.filter((m) => m.main.info.gpus && m.main.info.gpus.length).length;
    const tile = (big, label, sub) => `<div class="res-tile"><b>${big}</b><span>${label}</span><span>${sub}</span></div>`;
    $('#res-totals').innerHTML = [
      tile(`${sharing.length} of ${ms.length}`, ms.length === 1 ? 'device sharing' : 'devices sharing', sharing.length === ms.length ? 'all of them' : `${ms.length - sharing.length} connected but not sharing`),
      tile(nf.format(threads), 'processor threads', `of ${nf.format(cores)} cores on the sharing devices`),
      tile(mem ? `${gb(mem)} GB` : '—', 'memory', mem ? `${gb(memFree)} GB free on ${memMs.length} ${memMs.length === 1 ? 'device' : 'devices'}` : 'not reported'),
      tile(disk ? `${gb(disk)} GB` : '—', 'free disk for pooled storage', disk ? 'on computers running the app' : 'add a computer to store files'),
      tile(nf.format(gpus), gpus === 1 ? 'device with graphics' : 'devices with graphics', 'listed per device below'),
    ].join('');
    const here = ms.find((m) => m.here);
    $('#ov-sub').textContent = ms.length
      ? `${ms.length} ${ms.length === 1 ? 'device is' : 'devices are'} in this pool. You are on ${here ? here.main.name : S.name}.`
      : 'No devices yet.';

    $('#machines').innerHTML = ms.map((m) => {
      const i = m.main.info;
      const badges = [
        m.here ? '<span class="badge here">You are here</span>' : '',
        m.coordinator ? '<span class="badge">Runs the pool</span>' : '',
        m.terminal ? '<span class="badge">Hosts a terminal</span>' : '',
        m.stale ? '<span class="badge warn">Not responding</span>' : m.busy ? `<span class="badge ok">Working on ${m.busy}</span>` : m.sharing ? '<span class="badge ok">Sharing</span>' : '<span class="badge">Not sharing</span>',
      ].join('');
      const osText = m.terminal ? m.terminal.workspace.system : i.platform;
      const pct = i.cores ? Math.min(100, (m.threads / i.cores) * 100) : 0;
      const memText = i.memoryGB ? `${gb(i.memoryGB)} GB${i.memoryFreeGB ? `<small>${gb(i.memoryFreeGB)} GB free</small>` : ''}` : naText(['iPhone', 'iPad'].includes(i.platform) ? 'Safari does not report it' : 'Not reported');
      const diskText = m.agent && m.agent.info.diskFreeGB ? `${gb(m.agent.info.diskFreeGB)} GB free<small>holds pooled files</small>`
        : i.storageGB ? `${gb(i.storageGB)} GB<small>browser storage only</small>` : naText('Not reported');
      const gpuText = i.gpus && i.gpus.length ? esc(i.gpus.join(', ')) : naText('None');
      const via = m.list.map((d) => {
        const how = d.kind === 'agent' ? 'YourHarness app' : 'Browser tab';
        const state = d.stale ? 'not responding' : d.sharing ? `sharing ${d.threads} ${d.threads === 1 ? 'thread' : 'threads'}` : 'connected, not sharing';
        return `<span><b>${how}</b>: ${state}${d.id === S.id ? ' (this window)' : ''}</span>`;
      }).join('');
      const actions = [
        m.terminal ? `<a class="btn ghost small" href="#terminal" data-open-host="${esc(m.terminal.id)}">Open terminal here</a>` : '',
        m.main.files ? `<a class="btn ghost small" href="#files" data-open-files="${esc(m.main.id)}">Browse files</a>` : '',
      ].join('');
      const load = (m.agent && m.agent.load) || {};
      const cpu = typeof load.cpu === 'number' ? load.cpu : null;
      const memUsed = typeof load.memUsedGB === 'number' && i.memoryGB ? load.memUsedGB : null;
      const live = m.agent
        ? `<div class="live">
            <span class="lbl">CPU</span><span class="meter"><i style="width:${cpu ?? 0}%"></i></span><span class="val">${cpu === null ? '…' : `${Math.round(cpu)}%`}</span>
            <span class="lbl">Memory</span><span class="meter"><i style="width:${memUsed === null ? 0 : Math.min(100, (memUsed / i.memoryGB) * 100).toFixed(0)}%"></i></span><span class="val">${memUsed === null ? '…' : `${gb(memUsed)} / ${gb(i.memoryGB)} GB`}</span>
          </div>
          <canvas class="spark" data-spark="${esc(m.agent.id)}" height="40" aria-label="CPU use over the last two minutes"></canvas>`
        : `<div class="live"><span class="lbl">Activity</span><span class="meter"><i style="width:${m.threads ? Math.min(100, (m.busy / m.threads) * 100) : 0}%"></i></span><span class="val">${m.busy ? `${m.busy} piece${m.busy === 1 ? '' : 's'}` : 'idle'}</span></div>`;
      return `<article class="machine" style="--wire:${esc(m.main.color)}" data-off="${!m.sharing && !m.terminal}">
        <div class="machine-head"><span class="machine-name">${esc(m.main.name)}</span><span class="machine-os">${esc(osText)}</span><div class="badges">${badges}</div></div>
        <dl class="shares">
          <div><dt>Processor</dt><dd>${m.sharing ? `${m.threads} of ${i.cores} threads` : naText(`0 of ${i.cores} threads`)}<span class="res-bar"><i style="width:${pct.toFixed(0)}%"></i></span></dd></div>
          <div><dt>Memory</dt><dd>${memText}</dd></div>
          <div><dt>Storage</dt><dd>${diskText}</dd></div>
          <div><dt>Graphics</dt><dd>${gpuText}</dd></div>
        </dl>
        ${live}
        <div class="via">${via}<span>${nf.format(m.done)} pieces computed so far</span></div>
        ${actions ? `<div class="machine-actions">${actions}</div>` : ''}
      </article>`;
    }).join('') || '<p class="empty">No devices are connected.</p>';

    // the detailed table on the Devices page: one row per connection
    const list = S.devices.slice().sort((a, b) => (b.id === S.id) - (a.id === S.id) || a.joinedAt - b.joinedAt);
    $('#res-table tbody').innerHTML = list.map((d) => {
      const i = d.info;
      const off = !d.sharing || d.stale;
      const pct = i.cores ? Math.min(100, (d.threads / i.cores) * 100) : 0;
      const memCell = i.memoryGB ? `${gb(i.memoryGB)} GB${i.memoryFreeGB ? `<span class="res-sub">${gb(i.memoryFreeGB)} GB free</span>` : ''}` : naText('Not reported');
      const diskCell = d.kind === 'agent' ? (i.diskFreeGB ? `${gb(i.diskFreeGB)} GB free` : naText('Not reported'))
        : (i.storageGB ? `up to ${gb(i.storageGB)} GB<span class="res-sub">browser storage</span>` : naText('Not reported'));
      const status = d.stale ? 'not responding' : !d.sharing ? 'not sharing' : null;
      const action = d.id === S.id ? '<span class="res-kind">This window</span>'
        : `<button class="btn ghost small" type="button" data-remove="${esc(d.id)}" data-name="${esc(d.name)}">Remove</button>`;
      return `<tr style="--wire:${esc(d.color)}" data-off="${off}">
        <td><span class="res-name">${esc(d.name)}</span><span class="res-kind">${d.kind === 'agent' ? 'YourHarness app' : 'Browser tab'}, ${esc(i.platform)}${status ? `, ${status}` : ''}</span></td>
        <td>${d.sharing ? `${d.threads} of ${i.cores} threads` : naText('not sharing')}<span class="res-bar"><i style="width:${d.sharing ? pct.toFixed(0) : 0}%"></i></span></td>
        <td>${memCell}</td><td>${diskCell}</td><td>${i.gpus && i.gpus.length ? esc(i.gpus.join(', ')) : naText('None')}</td><td>${action}</td></tr>`;
    }).join('');

    renderTopbar();
    renderOverviewJob();
    renderProcs();
    drawSparks();
  }

  // CPU history per computer, sampled every 2 seconds, drawn as a small line under each card
  const SPARK = new Map();
  setInterval(() => {
    for (const d of S.devices) {
      if (d.kind !== 'agent' || !d.load || typeof d.load.cpu !== 'number') continue;
      const h = SPARK.get(d.id) || [];
      h.push(d.load.cpu);
      if (h.length > 60) h.shift();
      SPARK.set(d.id, h);
    }
    drawSparks();
  }, 2000);
  function drawSparks() {
    document.querySelectorAll('canvas[data-spark]').forEach((cv) => {
      const h = SPARK.get(cv.dataset.spark) || [];
      const w = cv.clientWidth || 300;
      const dpr = window.devicePixelRatio || 1;
      cv.width = Math.round(w * dpr);
      cv.height = Math.round(40 * dpr);
      const ctx = cv.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, 40);
      const color = getComputedStyle(cv.closest('.machine')).getPropertyValue('--wire').trim() || '#2563C9';
      ctx.strokeStyle = cssVar('--line-soft');
      ctx.beginPath(); ctx.moveTo(0, 39.5); ctx.lineTo(w, 39.5); ctx.stroke();
      if (h.length < 2) return;
      const x = (k) => w - (h.length - 1 - k) * (w / 59);
      const y = (v) => 38 - (v / 100) * 34;
      ctx.beginPath();
      h.forEach((v, k) => (k ? ctx.lineTo(x(k), y(v)) : ctx.moveTo(x(k), y(v))));
      ctx.lineTo(x(h.length - 1), 39); ctx.lineTo(x(0), 39); ctx.closePath();
      ctx.globalAlpha = 0.18; ctx.fillStyle = color; ctx.fill(); ctx.globalAlpha = 1;
      ctx.beginPath();
      h.forEach((v, k) => (k ? ctx.lineTo(x(k), y(v)) : ctx.moveTo(x(k), y(v))));
      ctx.strokeStyle = color; ctx.lineWidth = 1.5; ctx.stroke();
    });
  }

  const myMachineName = () => { const m = machines().find((x) => x.here); return m ? m.main.name : S.name; };
  function renderTopbar() {
    const conn = S.conn || 'connecting';
    $('#conn').textContent = conn === 'online' ? `Pool${S.rtt != null ? ` ${S.rtt} ms` : ''}` : conn === 'offline' ? 'Reconnecting' : 'Connecting';
    $('#conn').title = conn === 'online' ? `Connected to the pool at ${(S.address || location.origin).replace(/^https?:\/\//, '')}. ${S.rtt != null ? `Messages take ${S.rtt} ms to come back.` : ''}` : 'Not connected to the pool.';
    const agents = S.devices.filter((d) => d.kind === 'agent' && d.load && typeof d.load.cpu === 'number' && !d.stale);
    const cores = agents.reduce((a, d) => a + d.info.cores, 0);
    const cpu = cores ? agents.reduce((a, d) => a + d.load.cpu * d.info.cores, 0) / cores : null;
    $('#pb-cpu').innerHTML = cpu === null ? '' : `CPU <span class="pb-meter"><i style="width:${cpu.toFixed(0)}%"></i></span> <b>${Math.round(cpu)}%</b>`;
    $('#pb-cpu').title = cpu === null ? '' : `Average processor use across ${agents.length} ${agents.length === 1 ? 'computer' : 'computers'} (${cores} cores)`;
    $('#me-chip').innerHTML = `You are on <b>${esc(myMachineName())}</b>`;
    $('#me-chip').style.setProperty('--me-color', S.color || 'var(--ok)');
    const addr = (S.address || location.origin).replace(/^https?:\/\//, '');
    $('#code-chip').innerHTML = S.joinCode ? `Join code <b>${esc(S.joinCode)}</b> · ${esc(addr)}` : esc(addr);
  }

  // Programs the pool shell is running or ran recently, and the computer each is on
  function renderProcs() {
    const box = $('#procs');
    if (!box) return;
    const rows = (S.procs || []).slice(0, 12);
    box.innerHTML = rows.length ? rows.map((p) => {
      const dur = p.startedAt ? fmtDur((p.endedAt || Date.now()) - p.startedAt) : '';
      const state = p.status === 'running' ? 'running' : p.status === 'queued' ? 'waiting' : p.status === 'done' ? `exit ${p.code}` : p.status;
      return `<tr data-state="${esc(p.status)}"><td class="mono">${esc(p.cmd)}</td><td>${esc(p.runner || '—')}</td><td>${esc(state)}</td><td>${esc(dur)}</td></tr>`;
    }).join('') : '<tr><td colspan="4" class="hint">Nothing has run in the YourHarness OS shell yet.</td></tr>';
  }

  // The latest run, split by device: the plainest proof that devices really share the work.
  function renderOverviewJob() {
    const j = S.jobs.slice().reverse().find((x) => x.status !== 'cancelled');
    const box = $('#ov-job');
    box.hidden = !j;
    if (!j) return;
    const per = Object.values(j.perDevice).sort((a, b) => b.chunks - a.chunks);
    const head = j.status === 'running' || j.status === 'queued'
      ? `<b>Running:</b> ${esc(j.title)}, ${nf.format(j.done)} of ${nf.format(j.total)} pieces`
      : `<b>Last run:</b> ${esc(j.title)}${j.status === 'done' && j.startedAt && j.finishedAt ? `, finished in ${fmtDur(j.finishedAt - j.startedAt)}` : `, ${esc(j.status)}`}`;
    box.innerHTML = `<p>${head}</p><a class="btn ghost small" href="#jobs">Open Jobs</a>
      <div class="split">${per.map((p) => `<div class="split-row" style="--wire:${esc(p.color)}"><span>${esc(p.name)}</span>
        <span class="meter"><i style="width:${j.total ? ((p.chunks / j.total) * 100).toFixed(1) : 0}%"></i></span>
        <span>${nf.format(p.chunks)} pieces · ${j.total ? Math.round((p.chunks / j.total) * 100) : 0}%</span></div>`).join('') || '<p class="hint">No device has finished a piece yet.</p>'}</div>`;
  }
  setInterval(() => { $('#pb-clock').textContent = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }, 1000);

  // -------------------------------------------------------------------------
  // Rendering: current run
  // -------------------------------------------------------------------------
  function fmtDur(ms) {
    if (ms < 1000) return `${Math.round(ms)} ms`;
    if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
    const m = Math.floor(ms / 60_000);
    return `${m} min ${Math.round((ms % 60_000) / 1000)} s`;
  }
  const elapsed = (j) => (j.startedAt ? (j.finishedAt || Date.now()) - j.startedAt : 0);

  function whyIdle(j) {
    const runtimes = j.runtimes || ['js', 'python'];
    const able = S.devices.some((d) => d.sharing && !d.stale && (d.runtimes || ['js']).some((r) => runtimes.includes(r)));
    if (able) return '';
    if (runtimes.length === 1 && runtimes[0] === 'python') return ' No agent is connected, so no device can run Python. Add a computer under "Add a device".';
    if (runtimes.length === 1 && runtimes[0] === 'js') return ' No browser is sharing, so no device can run JavaScript.';
    return ' No device is sharing right now, so nothing is being computed.';
  }

  function statusText(j) {
    switch (j.status) {
      case 'queued': return 'Waiting for the run ahead of it to finish.';
      case 'running': return `${nf.format(j.done)} of ${nf.format(j.total)} pieces done in ${fmtDur(elapsed(j))}.${whyIdle(j)}`;
      case 'done': return `Finished all ${nf.format(j.total)} pieces in ${fmtDur(elapsed(j))}.`;
      case 'failed': return `Stopped. ${j.error || ''}`;
      case 'cancelled': return `Cancelled after ${nf.format(j.done)} of ${nf.format(j.total)} pieces.`;
      default: return '';
    }
  }

  function resultHtml(j) {
    if (j.status !== 'done' || !j.result) return '';
    const r = j.result;
    switch (j.kind) {
      case 'mandelbrot': return `${nf.format(r.pixels)} pixels rendered.`;
      case 'primes': return `${nf.format(r.count)} primes below ${nf.format(j.params.limit)}.`;
      case 'pi': return `π ≈ ${r.estimate.toFixed(7)}, off by ${Math.abs(r.estimate - Math.PI).toExponential(1)}.`;
      case 'grep': return `${nf.format(r.count)} ${r.count === 1 ? 'match' : 'matches'} for “${esc(j.params.term)}” in ${esc(j.params.file)}.`;
      case 'custom':
      case 'pymap': {
        const text = typeof r.value === 'number' ? nf.format(r.value) : JSON.stringify(r.value, null, 2);
        const clipped = text.length > 4000 ? `${text.slice(0, 4000)}\n…` : text;
        return typeof r.value === 'number' ? `Result: ${esc(clipped)}` : `<pre>${esc(clipped)}</pre>`;
      }
      default: return '';
    }
  }

  function renderCurrent(full) {
    const j = viewJob();
    $('#cur-empty').hidden = !!j;
    $('#cur-body').hidden = !j;
    const latest = S.jobs[S.jobs.length - 1];
    $('#follow-btn').hidden = !j || !latest || latest.id === j.id;
    $('#cancel-btn').hidden = !j || !(j.status === 'running' || j.status === 'queued');
    if (!j) return;

    $('#cur-title').textContent = j.title;
    $('#cur-status').textContent = statusText(j);
    $('#cur-result').innerHTML = resultHtml(j);

    const per = Object.entries(j.perDevice).sort((a, b) => b[1].chunks - a[1].chunks);
    const cpu = per.reduce((a, [, p]) => a + p.ms, 0);
    const wall = elapsed(j);
    $('#cur-parallel').textContent = j.status === 'done' && wall > 0 && cpu > 0
      ? `Used ${fmtDur(cpu)} of compute in ${fmtDur(wall)} of real time, about ${(cpu / wall).toFixed(1)} times one thread.`
      : '';
    $('#cur-breakdown tbody').innerHTML = per.map(([, p]) => `<tr style="--wire:${esc(p.color)}">
      <td><span class="swatch"></span>${esc(p.name)}</td>
      <td>${nf.format(p.chunks)}</td>
      <td>${Math.round((p.chunks / j.total) * 100)}%</td>
      <td>${fmtDur(p.ms)}</td></tr>`).join('');

    if (full) {
      layoutMap(j.total);
      for (let i = 0; i < j.total; i++) drawCell(i);
      const fc = $('#fractal');
      fc.hidden = j.kind !== 'mandelbrot';
      if (j.kind === 'mandelbrot') {
        setupFractal(j);
        S.view.values.forEach((v, i) => { if (v) drawBand(i); });
      } else {
        fractal = null;
      }
    }
  }

  // Chunk map: one square per piece, filled with the color of whoever computed it
  let map = null;
  function cssVar(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }
  function layoutMap(total) {
    const canvas = $('#chunk-map');
    const width = Math.max(200, canvas.parentElement.clientWidth);
    let cell = Math.floor(Math.sqrt((width * 110) / total));
    cell = Math.max(4, Math.min(18, cell));
    const cols = Math.max(1, Math.floor(width / cell));
    const rows = Math.ceil(total / cols);
    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.round(cols * cell * dpr);
    canvas.height = Math.round(rows * cell * dpr);
    canvas.style.width = `${cols * cell}px`;
    canvas.style.height = `${rows * cell}px`;
    map = { cell, cols, dpr, empty: cssVar('--line-soft') };
  }
  function drawCell(idx) {
    if (!map || !S.view) return;
    const ctx = $('#chunk-map').getContext('2d');
    const { cell, cols, dpr } = map;
    const gap = cell >= 8 ? 2 : 1;
    const x = (idx % cols) * cell;
    const y = Math.floor(idx / cols) * cell;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(x, y, cell, cell);
    ctx.fillStyle = S.view.colors[idx] || map.empty;
    ctx.fillRect(x, y, cell - gap, cell - gap);
  }

  // Fractal canvas: strips appear as they arrive; a mark on the left shows who drew each
  let fractal = null;
  const PALETTE = [[11, 27, 51], [38, 86, 140], [220, 230, 238], [224, 161, 0], [138, 59, 18], [11, 27, 51]];
  function buildLut(maxIter) {
    const lut = new Uint8ClampedArray((maxIter + 1) * 3);
    const cycle = 96;
    for (let i = 0; i <= maxIter; i++) {
      let r, g, b;
      if (i >= maxIter) { r = 8; g = 10; b = 14; } else {
        const t = ((i % cycle) / cycle) * (PALETTE.length - 1);
        const k = Math.floor(t);
        const f = t - k;
        const a = PALETTE[k];
        const c = PALETTE[Math.min(k + 1, PALETTE.length - 1)];
        r = a[0] + (c[0] - a[0]) * f; g = a[1] + (c[1] - a[1]) * f; b = a[2] + (c[2] - a[2]) * f;
      }
      lut[i * 3] = r; lut[i * 3 + 1] = g; lut[i * 3 + 2] = b;
    }
    return lut;
  }
  function setupFractal(j) {
    const { width, height, rows, maxIter } = j.params;
    const mark = Math.max(6, Math.round(width / 110));
    const canvas = $('#fractal');
    canvas.width = width + mark * 2;
    canvas.height = height;
    const ctx = canvas.getContext('2d');
    ctx.fillStyle = '#080A0E';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    fractal = { width, height, rows, maxIter, mark, lut: buildLut(maxIter), ctx };
  }
  function drawBand(idx) {
    if (!fractal || !S.view || !S.view.values) return;
    const b64 = S.view.values[idx];
    if (!b64) return;
    const iters = new Uint16Array(b64bytes(b64).buffer);
    const { width, rows, lut, mark, ctx } = fractal;
    const h = iters.length / width;
    if (!Number.isInteger(h) || h < 1) return;
    const img = ctx.createImageData(width, h);
    const px = img.data;
    for (let k = 0; k < iters.length; k++) {
      const li = Math.min(iters[k], fractal.maxIter) * 3;
      const o = k * 4;
      px[o] = lut[li]; px[o + 1] = lut[li + 1]; px[o + 2] = lut[li + 2]; px[o + 3] = 255;
    }
    const y0 = idx * rows;
    ctx.putImageData(img, mark * 2, y0);
    ctx.fillStyle = S.view.colors[idx] || '#555';
    ctx.fillRect(0, y0, mark, h);
  }

  let resizeTimer = null;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      fitTerminal();
      const j = viewJob();
      if (!j) return;
      layoutMap(j.total);
      for (let i = 0; i < j.total; i++) drawCell(i);
    }, 150);
  });

  // -------------------------------------------------------------------------
  // Rendering: history, files, this device, add a device
  // -------------------------------------------------------------------------
  function renderHistory() {
    const list = S.jobs.slice().reverse();
    const stateText = { queued: 'Waiting', running: 'Running', done: 'Done', failed: 'Stopped', cancelled: 'Cancelled' };
    $('#history').innerHTML = list.length
      ? list.map((j) => `<li><button type="button" data-job="${esc(j.id)}" aria-current="${S.view && S.view.jobId === j.id}">
          <span class="h-title">${esc(j.title)}</span>
          <span class="h-when">Started by ${esc(j.createdBy)}${j.startedAt && j.finishedAt ? `, took ${fmtDur(j.finishedAt - j.startedAt)}` : ''}</span>
          <span class="h-state">${esc(stateText[j.status] || j.status)}</span>
        </button></li>`).join('')
      : '<li class="empty">Finished runs show up here.</li>';
  }

  function fmtSize(n) {
    if (n < 1024) return `${n} B`;
    const units = ['KB', 'MB', 'GB', 'TB'];
    let v = n / 1024; let u = 0;
    while (v >= 1024 && u < units.length - 1) { v /= 1024; u++; }
    return `${v.toFixed(v < 10 ? 1 : 0)} ${units[u]}`;
  }
  function renderFiles() {
    // the "Search a file" job picks from the shared folder
    const sel = $('#g-file');
    const current = sel.value;
    sel.innerHTML = S.files.map((f) => `<option value="${esc(f.name)}">${esc(f.name)} (${fmtSize(f.size)})</option>`).join('');
    if (S.files.some((f) => f.name === current)) sel.value = current;
    $('#g-empty').hidden = S.files.length > 0;
  }

  // -------------------------------------------------------------------------
  // Files app: each computer's home folder, pooled storage, and the shared folder
  // -------------------------------------------------------------------------
  const FM = { loc: null, path: '', entries: [], info: null, busy: false, sending: null }; // loc.type: 'os' (/pool), 'fs' (a computer), 'shared'
  const FOLDER_SVG = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 6h7l2 2h9v11H3z"/></svg>';
  const FILE_SVG = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 3h8l4 4v14H6z"/><path d="M14 3v4h4"/></svg>';
  const fileComputers = () => machines().map((m) => m.list.find((d) => d.files)).filter(Boolean);
  const fmtWhen = (ms) => (ms ? new Date(ms).toLocaleString([], { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '');

  function fmRenderLocations() {
    const mine = myHost();
    const comps = fileComputers();
    if (!FM.loc) FM.loc = { type: 'os' };
    const cur = FM.loc;
    $('#fm-locs').innerHTML = comps.map((d) => `<li><button class="fm-loc" type="button" data-loc="fs" data-device="${esc(d.id)}" style="--wire:${esc(d.color)}"
        aria-current="${cur.type === 'fs' && cur.device === d.id}"><b>${esc(d.name)}${d.host && d.host === mine ? ' (this computer)' : ''}</b><span>${esc(d.workspace ? d.workspace.system : d.info.platform)}, home folder</span></button></li>`).join('')
      || '<li class="hint">No computer is sharing its files yet. Computers running the YourHarness app appear here.</li>';
    const coord = S.devices.find((d) => d.coordinator);
    $('#fm-locs-pool').innerHTML = `<li><button class="fm-loc" type="button" data-loc="os" aria-current="${cur.type === 'os'}" style="--wire:var(--ink)"><b>YourHarness OS · /pool</b><span>belongs to the pool, kept on ${Math.min(2, S.pool.agents) || 0} of ${S.pool.agents} ${S.pool.agents === 1 ? 'computer' : 'computers'}</span></button></li>
      <li><button class="fm-loc" type="button" data-loc="shared" aria-current="${cur.type === 'shared'}" style="--wire:var(--muted)"><b>Shared folder</b><span>on ${esc(coord ? coord.name : 'the pool computer')}, used by Search</span></button></li>`;
  }

  function fmDeviceName(id) { const d = S.devices.find((x) => x.id === id); return d ? d.name : 'that computer'; }

  async function fmLoad(keepStatus = false) {
    fmRenderLocations();
    const loc = FM.loc;
    if (loc.type !== 'fs' && loc.type !== 'os') { fmRender(); return; }
    FM.busy = true;
    const kept = keepStatus === true ? $('#fm-status').textContent : '';
    if (!kept) $('#fm-status').textContent = 'Loading…';
    try {
      const res = await fetch(loc.type === 'os' ? `/api/os/list?path=${encodeURIComponent(FM.path)}` : `/api/fs/${encodeURIComponent(loc.device)}?path=${encodeURIComponent(FM.path)}`, { credentials: 'same-origin' });
      const body = await res.json();
      if (!res.ok) throw new Error(body.error || `The pool answered ${res.status}.`);
      if (FM.loc !== loc) return;
      FM.path = body.path;
      FM.entries = body.entries;
      FM.info = body;
      $('#fm-status').textContent = body.truncated ? 'This folder has more items than can be shown.' : kept;
    } catch (err) {
      FM.entries = [];
      FM.info = null;
      $('#fm-status').textContent = err.message;
    } finally {
      FM.busy = false;
    }
    fmRender();
  }

  const fmApi = (loc) => (loc.type === 'os' ? {
    download: (path) => `/api/os/file?path=${encodeURIComponent(path)}`,
    upload: (path) => `/api/os/file?path=${encodeURIComponent(path)}`,
    mkdir: '/api/os/mkdir', rename: '/api/os/rename',
    remove: (path, dir) => `/api/os/file?path=${encodeURIComponent(path)}${dir ? '&recursive=1' : ''}`,
  } : {
    download: (path) => `/api/fs/${encodeURIComponent(loc.device)}/download?path=${encodeURIComponent(path)}`,
    upload: (path) => `/api/fs/${encodeURIComponent(loc.device)}/upload?path=${encodeURIComponent(path)}`,
    mkdir: `/api/fs/${encodeURIComponent(loc.device)}/mkdir`, rename: `/api/fs/${encodeURIComponent(loc.device)}/rename`,
    remove: (path, dir) => `/api/fs/${encodeURIComponent(loc.device)}?path=${encodeURIComponent(path)}${dir ? '&recursive=1' : ''}`,
  });
  const fmWhere = (loc) => (loc.type === 'os' ? `YourHarness OS /pool/${FM.path ? `${FM.path}/` : ''}` : `${fmDeviceName(loc.device)} ~/${FM.path ? `${FM.path}/` : ''}`);

  function fmRender() {
    const loc = FM.loc || { type: 'os' };
    const showHidden = $('#fm-show-hidden').checked;
    const folderView = loc.type === 'fs' || loc.type === 'os';
    $('#fm-up').disabled = !folderView || !FM.path;
    $('#fm-new').hidden = !folderView;
    $('.fm-hidden').hidden = loc.type !== 'fs';
    let crumbs = '';
    if (folderView) {
      const parts = FM.path ? FM.path.split('/') : [];
      const rootName = loc.type === 'os' ? 'YourHarness OS /pool' : `${fmDeviceName(loc.device)} ~`;
      crumbs = `<button type="button" data-crumb="">${esc(rootName)}</button>` + parts.map((part, k) =>
        `<span class="sep">/</span><button type="button" data-crumb="${esc(parts.slice(0, k + 1).join('/'))}">${esc(part)}</button>`).join('');
    } else {
      crumbs = '<span>Shared folder</span>';
    }
    $('#fm-crumbs').innerHTML = crumbs;
    $('#wsub-files').textContent = folderView ? fmWhere(loc).replace(/\/$/, '') : 'Shared folder';

    let rows = '';
    if (folderView) {
      const api = fmApi(loc);
      const entries = FM.entries.filter((e) => showHidden || !e.hidden);
      $('#fm-info').textContent = loc.type === 'os'
        ? `This is the disk of YourHarness OS. It belongs to the pool, not to a computer: files here are cut into pieces kept on ${Math.min(2, S.pool.agents)} of the ${S.pool.agents} computers, and every command you run in the OS shell sees them as /pool. Uploads go into this folder.`
        : (FM.info ? `Files on ${fmDeviceName(loc.device)}. Uploads go into this folder. ${fmtSize(FM.info.free)} free on its disk.` : '');
      rows = entries.map((e) => {
        const path = FM.path ? `${FM.path}/${e.name}` : e.name;
        const name = e.dir ? `<button class="fm-name" type="button" data-open-dir="${esc(path)}">${FOLDER_SVG}${esc(e.name)}</button>`
          : `<span class="fm-name">${FILE_SVG}${esc(e.name)}</span>${loc.type === 'os' && e.on ? `<span class="fm-on">stored on ${esc(e.on.join(' and ') || 'no connected computer')}${e.available === false ? ' · offline' : ''}</span>` : ''}`;
        const acts = e.dir
          ? `<button class="btn ghost" type="button" data-rename="${esc(path)}">Rename</button><button class="btn ghost danger" type="button" data-del="${esc(path)}" data-dir="1">Delete</button>`
          : `${e.available === false ? '' : `<a class="btn ghost" href="${api.download(path)}" download="${esc(e.name)}">Download</a>`}
             ${loc.type === 'fs' ? `<button class="btn ghost" type="button" data-send="${esc(path)}">Send to…</button>` : ''}
             <button class="btn ghost" type="button" data-rename="${esc(path)}">Rename</button>
             <button class="btn ghost danger" type="button" data-del="${esc(path)}">Delete</button>`;
        return `<tr><td>${name}</td><td>${e.dir ? '' : fmtSize(e.size)}</td><td>${fmtWhen(e.mtime)}</td><td><div class="fm-acts">${acts}</div></td></tr>`;
      }).join('') || `<tr><td colspan="4" class="hint">${FM.busy ? 'Loading…' : loc.type === 'os' ? 'This folder is empty. Upload files here, or make some in the YourHarness OS terminal.' : 'This folder is empty.'}</td></tr>`;
    } else {
      $('#fm-info').textContent = 'One folder on the computer that runs the pool. Every device can read it, and "Search a file" in Jobs reads from here.';
      rows = S.files.map((f) => `<tr><td><span class="fm-name">${FILE_SVG}${esc(f.name)}</span>${f.by ? `<span class="fm-on">added by ${esc(f.by)}</span>` : ''}</td>
          <td>${fmtSize(f.size)}</td><td>${fmtWhen(f.mtime)}</td>
          <td><div class="fm-acts"><a class="btn ghost" href="/api/files/${encodeURIComponent(f.name)}" download="${esc(f.name)}">Download</a>
          <button class="btn ghost" type="button" data-search="${esc(f.name)}">Search</button>
          <button class="btn ghost danger" type="button" data-shared-del="${esc(f.name)}">Delete</button></div></td></tr>`).join('')
        || '<tr><td colspan="4" class="hint">The shared folder is empty.</td></tr>';
    }
    $('#fm-rows').innerHTML = rows;
  }

  // A small in-page dialog for names; the desktop app's engine has no window.prompt()
  function askText(title, initial = '', okLabel = 'OK') {
    return new Promise((resolve) => {
      const f = $('#fm-ask');
      const input = $('#fm-ask-input');
      $('#fm-ask-title').textContent = title;
      $('#fm-ask-ok').textContent = okLabel;
      input.value = initial;
      f.hidden = false;
      input.focus();
      input.select();
      const done = (v) => { f.hidden = true; f.onsubmit = null; $('#fm-ask-cancel').onclick = null; resolve(v); };
      f.onsubmit = (e) => { e.preventDefault(); done(input.value.trim() || null); };
      $('#fm-ask-cancel').onclick = () => done(null);
    });
  }

  async function fmPost(url, body) {
    const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), credentials: 'same-origin' });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `The pool answered ${res.status}.`);
    return data;
  }

  function renderMe() {
    $('#me').style.setProperty('--me', S.color || 'var(--ok)');
    $('#me-share').checked = S.sharing;
    const r = $('#me-threads');
    r.max = MAX_THREADS;
    r.value = S.threads;
    $('#me-threads-out').textContent = `${S.threads} of ${CORES} available`;
    if (document.activeElement !== $('#me-name')) $('#me-name').value = S.name;
    renderAwake();
  }

  function renderAdd() {
    if (!S.joinToken) return;
    const origin = S.address || location.origin; // the LAN address, even when this window is on 127.0.0.1
    const link = `${origin}/#join=${S.joinToken}`;
    $('#add-addr').value = origin.replace(/^https?:\/\//, '');
    $('#add-code').value = S.joinCode || 'Off: too many wrong codes. Press New code.';
    $('#add-link').value = link;
    $('#add-cmd').value = `curl -fsSL ${origin}/install.sh | bash -s -- --join ${S.joinCode || `'${link}'`} --workspace`;
    $('#add-win').value = `irm ${origin}/install.ps1 | iex`;
  }

  function renderAll() {
    renderDevices();
    renderMe();
    renderCurrent(true);
    renderHistory();
    renderFiles();
    renderAdd();
    renderWorkspace();
  }

  // -------------------------------------------------------------------------
  // Screens
  // -------------------------------------------------------------------------
  function showJoin(error) {
    $('#boot').hidden = true;
    $('#app').hidden = true;
    $('#join').hidden = false;
    $('#join-name').value = S.name;
    $('#join-error').textContent = error || '';
    (S.name ? $('#join-link') : $('#join-name')).focus();
  }
  function showApp() {
    $('#boot').hidden = true;
    $('#join').hidden = true;
    const first = $('#app').hidden;
    $('#app').hidden = false;
    if (first) WM.restore();
    resizePool(S.sharing ? S.threads : 0);
    updateWakeLock();
  }
  function signedOut(reason) {
    S.joined = false;
    if (T.open) closeTerminal();
    dropAllWork();
    updateWakeLock();
    showJoin(reason);
  }

  // -------------------------------------------------------------------------
  // Events
  // -------------------------------------------------------------------------
  $('#join-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const name = $('#join-name').value.trim();
    const token = parseJoin($('#join-link').value);
    if (!name) { $('#join-error').textContent = 'Give this device a name so you can tell it apart.'; return; }
    if (!token) { $('#join-error').textContent = 'Enter the 4-digit join code shown in the coordinator terminal, or paste the join link.'; return; }
    S.name = name;
    store.set('name', name);
    $('#join-error').textContent = '';
    try {
      await joinWith(token, name);
      S.joined = true;
      connect();
    } catch (err) {
      $('#join-error').textContent = err.message;
    }
  });

  $('#replaced-btn').addEventListener('click', () => {
    resizePool(S.sharing ? S.threads : 0);
    S.retry = 0;
    connect();
  });

  $('#me-share').addEventListener('change', (e) => {
    S.sharing = e.target.checked;
    store.set('sharing', S.sharing ? 'on' : 'off');
    if (!S.sharing) {
      // Hand back pieces that have not started yet so other devices pick them up.
      for (const t of pool.queue) send({ type: 'release', jobId: t.jobId, idx: t.idx });
      pool.queue = [];
    }
    resizePool(S.sharing ? S.threads : 0);
    send({ type: 'update', sharing: S.sharing });
    updateWakeLock();
  });
  $('#me-threads').addEventListener('input', (e) => {
    S.threads = clampThreads(Number(e.target.value));
    $('#me-threads-out').textContent = `${S.threads} of ${CORES} available`;
  });
  $('#me-threads').addEventListener('change', () => {
    store.set('threads', S.threads);
    if (S.sharing) resizePool(S.threads);
    send({ type: 'update', threads: S.threads });
  });
  $('#me-name').addEventListener('change', (e) => {
    const name = e.target.value.trim();
    if (!name) { e.target.value = S.name; return; }
    S.name = name;
    store.set('name', name);
    send({ type: 'update', name });
  });
  $('#me-leave').addEventListener('click', async () => {
    if (!confirm('Leave the pool on this device? You will need the join link to come back.')) return;
    try { await fetch('/api/leave', { method: 'POST', credentials: 'same-origin' }); } catch { /* offline: the cookie stays until it expires */ }
    const ws = S.ws;
    S.ws = null;
    if (ws) ws.close();
    signedOut('You left the pool. Open the join link to come back.');
  });

  $('#res-table').addEventListener('click', async (e) => {
    const b = e.target.closest('button[data-remove]');
    if (!b) return;
    if (!confirm(`Remove ${b.dataset.name} from the pool? It will need the join link to come back.`)) return;
    const res = await fetch(`/api/devices/${encodeURIComponent(b.dataset.remove)}`, { method: 'DELETE', credentials: 'same-origin' });
    if (!res.ok) {
      let msg = `Could not remove ${b.dataset.name}.`;
      try { msg = (await res.json()).error || msg; } catch { /* not JSON */ }
      alert(msg);
    }
  });

  // Workspace
  $('#ws-open').addEventListener('click', () => openTerminal(T.kind, T.hostId));
  $('#ws-hosts').addEventListener('click', (e) => {
    const b = e.target.closest('button.host-tab');
    if (!b) return;
    if (b.dataset.kind === 'os') { T.kind = 'os'; openTerminal('os'); }
    else { T.kind = 'direct'; T.hostId = b.dataset.host; openTerminal('direct', T.hostId); }
  });
  $('#ws-close').addEventListener('click', closeTerminal);
  $('#machines').addEventListener('click', (e) => {
    const a = e.target.closest('[data-open-host]');
    if (!a) return;
    T.autoDone = true;
    setTimeout(() => openTerminal('direct', a.dataset.openHost), 0); // after the window opens
  });
  $('#ws-close').addEventListener('click', closeTerminal);
  document.querySelector('.keybar').addEventListener('click', (e) => {
    const b = e.target.closest('button[data-seq]');
    if (!b || !T.sid) return;
    send(termMsg('in', { data: KEYS[b.dataset.seq] }));
    T.term.focus();
  });

  // A fresh join code. It also turns code joins back on after too many wrong guesses.
  $('#add-new-code').addEventListener('click', async () => {
    const status = $('#add-status');
    try {
      const res = await fetch('/api/join-code', { method: 'POST', credentials: 'same-origin' });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(body.error || `HTTP ${res.status}`);
      S.joinCode = body.joinCode;
      renderAdd();
      status.textContent = 'New code made. The old one no longer works.';
    } catch (err) {
      status.textContent = `Couldn't make a new code: ${err.message}`;
    }
  });

  // Copy buttons. navigator.clipboard needs HTTPS, so fall back to selecting the text.
  document.addEventListener('click', async (e) => {
    const b = e.target.closest('button[data-copy]');
    if (!b) return;
    const field = document.getElementById(b.dataset.copy);
    const status = $('#add-status');
    try {
      if (!navigator.clipboard) throw new Error('no clipboard');
      await navigator.clipboard.writeText(field.value);
      status.textContent = 'Copied.';
    } catch {
      field.focus();
      field.select();
      const ok = (() => { try { return document.execCommand('copy'); } catch { return false; } })();
      status.textContent = ok ? 'Copied.' : 'Selected. Press Ctrl+C or ⌘C to copy.';
    }
  });

  // Job type switching
  document.querySelectorAll('input[name="kind"]').forEach((r) => r.addEventListener('change', showKind));
  function selectedKind() { return $('input[name="kind"]:checked').value; }
  function showKind() {
    const kind = selectedKind();
    document.querySelectorAll('#run-form fieldset').forEach((f) => { f.hidden = f.dataset.kind !== kind; });
    $('#run-error').textContent = '';
  }
  $('#c-lang').value = store.get('lang') === 'python' ? 'python' : 'js';
  $('#c-code').value = DEFAULT_CODE[$('#c-lang').value];
  $('#c-lang').addEventListener('change', (e) => {
    const lang = e.target.value;
    store.set('lang', lang);
    const code = $('#c-code').value;
    if (!code.trim() || Object.values(DEFAULT_CODE).includes(code)) $('#c-code').value = DEFAULT_CODE[lang];
  });

  function collectParams(kind) {
    switch (kind) {
      case 'mandelbrot': {
        const [width, height] = $('#f-size').value.split('x').map(Number);
        return { width, height, maxIter: Number($('#f-iter').value), rows: 8, ...FRACTAL_PLACES[$('#f-loc').value] };
      }
      case 'primes':
        return { limit: Number($('#p-limit').value), chunkSize: 1e7 };
      case 'pi': {
        const total = Number($('#pi-samples').value);
        return { chunks: 400, samples: Math.round(total / 400) };
      }
      case 'grep':
        return { file: $('#g-file').value, term: $('#g-term').value, ignoreCase: $('#g-case').checked };
      case 'custom':
        return { code: $('#c-code').value, chunks: Number($('#c-chunks').value), combine: $('#c-combine').value, lang: $('#c-lang').value };
      default:
        return {};
    }
  }

  $('#run-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const kind = selectedKind();
    const err = $('#run-error');
    err.textContent = '';
    if (kind === 'grep' && !S.files.length) { err.textContent = 'Add a file under Shared files first, then search it.'; return; }
    if (kind === 'grep' && !$('#g-term').value) { err.textContent = 'Enter the text to find.'; $('#g-term').focus(); return; }
    if (kind === 'custom') {
      const code = $('#c-code').value;
      const python = $('#c-lang').value === 'python';
      if (python ? !/def\s+run\s*\(/.test(code) : !/function\s+run\s*\(|run\s*=/.test(code)) {
        err.textContent = 'The code needs a function named run(chunk, total).';
        return;
      }
    }
    if (!S.ws || S.ws.readyState !== 1) { err.textContent = 'Not connected to the coordinator. Waiting to reconnect.'; return; }
    S.follow = true;
    send({ type: 'submit', job: { kind, params: collectParams(kind) } });
    const btn = $('#run-btn');
    btn.disabled = true;
    setTimeout(() => { btn.disabled = false; }, 600);
  });

  $('#cancel-btn').addEventListener('click', () => {
    const j = viewJob();
    if (j) send({ type: 'cancel', jobId: j.id });
  });
  $('#follow-btn').addEventListener('click', () => {
    S.follow = true;
    const latest = S.jobs[S.jobs.length - 1];
    if (latest) send({ type: 'getSnapshot', jobId: latest.id });
  });
  $('#history').addEventListener('click', (e) => {
    const b = e.target.closest('button[data-job]');
    if (!b) return;
    const latest = S.jobs[S.jobs.length - 1];
    S.follow = !!latest && latest.id === b.dataset.job;
    send({ type: 'getSnapshot', jobId: b.dataset.job });
    $('#current').scrollIntoView({ behavior: reduceMotion() ? 'auto' : 'smooth', block: 'start' });
  });

  // Files
  function uploadFile(file, onPct, base = '/api/files', url = null) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('PUT', url || `${base}/${encodeURIComponent(file.name)}`);
      xhr.withCredentials = true;
      xhr.upload.onprogress = (ev) => { if (ev.lengthComputable) onPct(Math.round((ev.loaded / ev.total) * 100)); };
      xhr.onload = () => {
        if (xhr.status < 300) return resolve();
        let msg = `The coordinator answered ${xhr.status}.`;
        try { msg = JSON.parse(xhr.responseText).error || msg; } catch { /* not JSON */ }
        reject(new Error(msg));
      };
      xhr.onerror = () => reject(new Error('The connection dropped.'));
      xhr.send(file);
    });
  }
  // Files app events
  document.addEventListener('click', (e) => {
    const loc = e.target.closest('.fm-loc');
    if (!loc) return;
    FM.loc = loc.dataset.loc === 'fs' ? { type: 'fs', device: loc.dataset.device } : { type: loc.dataset.loc };
    FM.path = '';
    fmLoad();
  });
  $('#fm-crumbs').addEventListener('click', (e) => {
    const b = e.target.closest('button[data-crumb]');
    if (b) { FM.path = b.dataset.crumb; fmLoad(); }
  });
  $('#fm-up').addEventListener('click', () => { FM.path = FM.path.split('/').slice(0, -1).join('/'); fmLoad(); });
  $('#fm-refresh').addEventListener('click', fmLoad);
  $('#fm-show-hidden').addEventListener('change', fmRender);
  $('#fm-new').addEventListener('click', async () => {
    const name = await askText('New folder', '', 'Create');
    if (!name) return;
    try {
      await fmPost(fmApi(FM.loc).mkdir, { path: FM.path ? `${FM.path}/${name}` : name });
      fmLoad();
    } catch (err) { $('#fm-status').textContent = err.message; }
  });
  $('#fm-upload').addEventListener('change', async (e) => {
    const files = [...e.target.files];
    e.target.value = '';
    const loc = FM.loc;
    const status = $('#fm-status');
    for (const f of files) {
      try {
        const folderView = loc.type === 'fs' || loc.type === 'os';
        const where = folderView ? fmWhere(loc) : 'the shared folder';
        const url = folderView ? fmApi(loc).upload(FM.path ? `${FM.path}/${f.name}` : f.name) : null;
        await uploadFile(f, (pct) => { status.textContent = `Sending ${f.name} to ${where}: ${pct}%`; }, '/api/files', url);
        status.textContent = `Saved ${f.name} to ${where}.`;
      } catch (err) {
        status.textContent = `Could not save ${f.name}. ${err.message}`;
      }
    }
    if (loc.type === 'fs' || loc.type === 'os') fmLoad(true);
  });
  $('#fm-rows').addEventListener('click', async (e) => {
    const t = e.target.closest('[data-open-dir],[data-rename],[data-del],[data-send],[data-shared-del],[data-search]');
    if (!t) return;
    const status = $('#fm-status');
    const dev = FM.loc && FM.loc.device;
    try {
      if (t.dataset.openDir !== undefined) { FM.path = t.dataset.openDir; fmLoad(); return; }
      if (t.dataset.rename) {
        const old = t.dataset.rename;
        const base = old.split('/').pop();
        const name = await askText(`Rename ${base}`, base, 'Rename');
        if (!name || name === base) return;
        await fmPost(fmApi(FM.loc).rename, { path: old, to: [...old.split('/').slice(0, -1), name].join('/') });
        fmLoad();
      } else if (t.dataset.del) {
        const path = t.dataset.del;
        const isDir = !!t.dataset.dir;
        if (!confirm(`Delete ${path.split('/').pop()} from ${FM.loc.type === 'os' ? 'YourHarness OS' : fmDeviceName(dev)}?${isDir ? ' Everything inside the folder is deleted too.' : ''} This can't be undone.`)) return;
        const res = await fetch(fmApi(FM.loc).remove(path, isDir), { method: 'DELETE', credentials: 'same-origin' });
        if (!res.ok) throw new Error((await res.json().catch(() => ({}))).error || 'Could not delete it.');
        fmLoad();
      } else if (t.dataset.send) {
        FM.sending = { device: dev, path: t.dataset.send };
        const others = fileComputers().filter((d) => d.id !== dev);
        if (!others.length) { status.textContent = 'There is no other computer to send to. Open the YourHarness app on another computer first.'; return; }
        $('#fm-send-to').innerHTML = others.map((d) => `<option value="${esc(d.id)}">${esc(d.name)}</option>`).join('');
        $('#fm-send-what').textContent = `${t.dataset.send.split('/').pop()} from ${fmDeviceName(dev)}`;
        $('#fm-send-err').textContent = '';
        $('#fm-send').hidden = false;
      } else if (t.dataset.sharedDel) {
        if (!confirm(`Delete ${t.dataset.sharedDel} from the shared folder?`)) return;
        await fetch(`/api/files/${encodeURIComponent(t.dataset.sharedDel)}`, { method: 'DELETE', credentials: 'same-origin' });
      } else if (t.dataset.search) {
        $('input[name="kind"][value="grep"]').checked = true;
        showKind();
        $('#g-file').value = t.dataset.search;
        WM.open('jobs');
      }
    } catch (err) { status.textContent = err.message; }
  });
  $('#fm-send-cancel').addEventListener('click', () => { $('#fm-send').hidden = true; });
  $('#fm-send').addEventListener('submit', async (e) => {
    e.preventDefault();
    const to = $('#fm-send-to').value;
    const dir = $('#fm-send-dir').value.trim();
    $('#fm-send-err').textContent = 'Sending…';
    try {
      const res = await fmPost('/api/fs-copy', { from: FM.sending, to: { device: to, dir } });
      $('#fm-send').hidden = true;
      $('#fm-status').textContent = `Copied to ${fmDeviceName(to)} ~/${res.path} (${fmtSize(res.size)}).`;
    } catch (err) { $('#fm-send-err').textContent = err.message; }
  });

  // -------------------------------------------------------------------------
  // Window manager: draggable, resizable windows on wide screens, full-screen apps on phones
  // -------------------------------------------------------------------------
  const APP_SIZES = { monitor: [720, 640], terminal: [860, 600], files: [920, 600], jobs: [1000, 680], devices: [940, 680] };
  const WM = (() => {
    let z = 10;
    const saved = (() => { try { return JSON.parse(store.get('wm') || '{}'); } catch { return {}; } })();
    const state = saved && typeof saved === 'object' ? saved : {};
    const desk = $('#desk');
    const win = (app) => $(`#win-${app}`);
    const phone = () => matchMedia('(max-width: 820px)').matches;
    const save = () => store.set('wm', JSON.stringify(state));
    const isOpen = (app) => !win(app).hidden;

    function place(app) {
      const el = win(app);
      const st = state[app] || (state[app] = {});
      if (phone()) return; // apps are full screen on phones; keep the desktop sizes for later
      const W = desk.clientWidth, H = desk.clientHeight;
      if (st.w == null) {
        const [w, h] = APP_SIZES[app] || [800, 600];
        const order = Object.keys(APP_SIZES).indexOf(app);
        st.w = Math.min(w, W - 24); st.h = Math.min(h, H - 24);
        st.x = Math.max(12, Math.min(W - st.w - 12, 24 + order * 36)); st.y = Math.max(12, Math.min(H - st.h - 12, 18 + order * 28));
      }
      st.w = Math.max(320, Math.min(st.w, W)); st.h = Math.max(220, Math.min(st.h, H));
      st.x = Math.max(0, Math.min(st.x, W - 120)); st.y = Math.max(0, Math.min(st.y, H - 40));
      Object.assign(el.style, { left: `${st.x}px`, top: `${st.y}px`, width: `${st.w}px`, height: `${st.h}px` });
      el.classList.toggle('max', !!st.max);
    }
    function focus(app) {
      document.querySelectorAll('.win').forEach((w) => w.classList.remove('focused'));
      const el = win(app);
      el.style.zIndex = String(++z);
      el.classList.add('focused');
      state.front = app;
      dockState();
    }
    function dockState() {
      document.querySelectorAll('.dock-item').forEach((b) => {
        const a = b.dataset.open;
        b.classList.toggle('running', isOpen(a) || (state[a] && state[a].min));
        b.classList.toggle('front', isOpen(a) && state.front === a);
      });
      $('#launcher').hidden = Object.keys(APP_SIZES).some(isOpen); // the app tiles show on an empty desktop only
    }
    function opened(app) {
      if (app === 'terminal') { renderWorkspace(); if (S.joined && S.devices.length) autoOpenTerminal(); requestAnimationFrame(fitTerminal); }
      if (app === 'files') fmLoad();
      if (app === 'jobs') requestAnimationFrame(() => { if (viewJob()) renderCurrent(true); });
      if (app === 'monitor') requestAnimationFrame(drawSparks);
    }
    function open(app) {
      if (!win(app)) return;
      if (phone()) Object.keys(APP_SIZES).forEach((a) => { if (a !== app && isOpen(a)) { win(a).hidden = true; } });
      const st = state[app] || (state[app] = {});
      st.open = true; st.min = false;
      win(app).hidden = false;
      place(app);
      focus(app);
      save();
      opened(app);
    }
    function minimize(app) {
      win(app).hidden = true;
      state[app] = { ...(state[app] || {}), open: true, min: true };
      save(); dockState();
    }
    function close(app) {
      win(app).hidden = true;
      state[app] = { ...(state[app] || {}), open: false, min: false };
      if (app === 'terminal') closeTerminal();
      save(); dockState();
    }
    function toggleMax(app) {
      const st = state[app] || (state[app] = {});
      st.max = !st.max;
      place(app); save();
      requestAnimationFrame(() => { fitTerminal(); drawSparks(); });
    }
    function restore() {
      const apps = Object.keys(APP_SIZES).filter((a) => state[a] && state[a].open && !state[a].min);
      if (!apps.length && !phone()) {
        // first visit on a wide screen: the monitor on the left, the terminal on the right
        const W = desk.clientWidth, H = desk.clientHeight;
        const half = Math.floor((W - 36) / 2);
        state.monitor = { x: 12, y: 12, w: Math.max(360, half), h: H - 24, open: true };
        state.terminal = { x: Math.min(W - 372, 24 + half), y: 12, w: Math.max(360, half), h: H - 24, open: true };
        apps.push('monitor', 'terminal');
      }
      apps.forEach((a) => { win(a).hidden = false; place(a); focus(a); });
      if (state.front && apps.includes(state.front)) focus(state.front);
      dockState();
      apps.forEach(opened);
    }

    // dragging and resizing with pointer events (mouse, pen and touch)
    desk.addEventListener('pointerdown', (e) => {
      const el = e.target.closest('.win');
      if (!el) return;
      const app = el.dataset.app;
      focus(app);
      if (phone()) return;
      const bar = e.target.closest('.win-bar');
      const grip = e.target.closest('.win-resize');
      if ((!bar && !grip) || e.target.closest('button') || (state[app] && state[app].max)) return;
      e.preventDefault();
      const st = state[app];
      const start = { px: e.clientX, py: e.clientY, x: st.x, y: st.y, w: st.w, h: st.h };
      el.setPointerCapture(e.pointerId);
      const move = (ev) => {
        if (grip) { st.w = start.w + ev.clientX - start.px; st.h = start.h + ev.clientY - start.py; }
        else { st.x = start.x + ev.clientX - start.px; st.y = start.y + ev.clientY - start.py; }
        place(app);
      };
      const up = () => {
        el.removeEventListener('pointermove', move);
        el.removeEventListener('pointerup', up);
        el.removeEventListener('pointercancel', up);
        save();
        if (grip) requestAnimationFrame(() => { fitTerminal(); drawSparks(); });
      };
      el.addEventListener('pointermove', move);
      el.addEventListener('pointerup', up);
      el.addEventListener('pointercancel', up);
    });
    desk.addEventListener('dblclick', (e) => {
      const bar = e.target.closest('.win-bar');
      if (bar && !e.target.closest('button') && !phone()) toggleMax(bar.closest('.win').dataset.app);
    });
    desk.addEventListener('click', (e) => {
      const b = e.target.closest('.win-bar button[data-act]');
      if (!b) return;
      const app = b.closest('.win').dataset.app;
      if (b.dataset.act === 'min') minimize(app);
      else if (b.dataset.act === 'max') toggleMax(app);
      else close(app);
    });
    document.addEventListener('click', (e) => {
      const b = e.target.closest('[data-open]');
      if (!b) return;
      const app = b.dataset.open;
      if (b.classList.contains('dock-item') && isOpen(app) && state.front === app && !phone()) minimize(app);
      else open(app);
    });
    window.addEventListener('resize', () => { Object.keys(APP_SIZES).forEach((a) => { if (isOpen(a)) place(a); }); dockState(); });
    return { open, close, minimize, isOpen, restore, dockState };
  })();

  $('#pb-home').addEventListener('click', () => {
    if (matchMedia('(max-width: 820px)').matches) Object.keys(APP_SIZES).forEach((a) => { if (WM.isOpen(a)) WM.minimize(a); });
    else WM.open('monitor');
  });
  // links inside apps such as href="#terminal" open that app's window
  document.addEventListener('click', (e) => {
    const a = e.target.closest('a[href^="#"]');
    if (!a) return;
    const app = a.getAttribute('href').slice(1);
    if (!APP_SIZES[app]) return;
    e.preventDefault();
    if (a.dataset.openFiles) { FM.loc = { type: 'fs', device: a.dataset.openFiles }; FM.path = ''; }
    WM.open(app);
  });
  $('#mon-test-btn').addEventListener('click', () => {
    if (!S.ws || S.ws.readyState !== 1) return;
    S.follow = true;
    send({ type: 'submit', job: { kind: 'primes', params: { limit: 3e9, chunkSize: 1e7 } } });
    $('#mon-test-btn').disabled = true;
    setTimeout(() => { $('#mon-test-btn').disabled = false; }, 1500);
  });
  if ('ResizeObserver' in window) new ResizeObserver(() => fitTerminal()).observe($('#term'));

  // -------------------------------------------------------------------------
  // Start: a join link in the address bar signs this device in; otherwise try
  // the cookie from an earlier visit.
  // -------------------------------------------------------------------------
  showKind();
  const fromLink = parseJoin(location.hash);
  if (fromLink) {
    history.replaceState(null, '', `${location.pathname}${location.search}`);
    joinWith(fromLink, S.name)
      .then(() => { S.joined = true; connect(); })
      .catch((err) => showJoin(err.message));
  } else {
    S.joined = true;
    connect();
  }
})();
