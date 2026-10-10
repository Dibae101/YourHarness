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
  setInterval(() => send({ type: 'beat' }), BEAT_MS);

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible' && S.joined && !S.replaced && !S.ws) {
      S.retry = 0;
      connect();
    }
    updateWakeLock();
  });

  function handle(m) {
    switch (m.type) {
      case 'welcome':
        S.retry = 0;
        S.id = m.id;
        S.color = m.color;
        S.joinToken = m.joinToken;
        S.joinCode = m.joinCode;
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
        if (T.open) openTerminal();
        else maybeAutoOpen();
        break;
      case 'denied':
        S.deniedReason = m.reason || '';
        break;
      case 'devices':
        S.devices = m.devices;
        renderDevices();
        renderWorkspace();
        maybeAutoOpen();
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
        break;
      case 'pool':
        S.pool = m.pool;
        renderPool();
        break;
      case 'error':
        $('#run-error').textContent = m.message;
        break;
      case 'term.opened':
      case 'term.out':
      case 'term.exit':
      case 'term.error':
        onTerminal(m);
        break;
      default:
        break;
    }
  }

  function setConn(state) {
    const el = $('#conn');
    el.dataset.state = state;
    el.textContent = { online: 'Connected', offline: 'Reconnecting', connecting: 'Connecting' }[state];
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
  const T = { term: null, fit: null, sid: null, hostId: null, ref: null, open: false, max: false, loading: null };
  const KEYS = { esc: '\x1b', tab: '\t', ctrlc: '\x03', ctrld: '\x04', up: '\x1b[A', down: '\x1b[B', right: '\x1b[C', left: '\x1b[D' };

  function loadScript(src) {
    return new Promise((resolve, reject) => {
      const el = document.createElement('script');
      el.src = src;
      el.onload = resolve;
      el.onerror = () => reject(new Error(`Could not load ${src}`));
      document.head.appendChild(el);
    });
  }
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
        T.term.onData((d) => { if (T.sid) send({ type: 'term.in', sid: T.sid, data: d }); });
        T.term.onResize(({ cols, rows }) => { if (T.sid) send({ type: 'term.resize', sid: T.sid, cols, rows }); });
      });
    }
    return T.loading;
  }
  // Landing in the workspace is the point, so open the terminal as soon as a computer can host it.
  function maybeAutoOpen() {
    if (T.autoDone || T.open || !S.devices.some((d) => d.workspace)) return;
    T.autoDone = true;
    openTerminal();
  }
  async function openTerminal() {
    T.open = true;
    renderWorkspace();
    try {
      await ensureTerminal();
    } catch (err) {
      $('#ws-status').textContent = err.message;
      return;
    }
    fitTerminal();
    if (T.sid) send({ type: 'term.close', sid: T.sid });
    T.sid = null;
    T.ref = newId();
    T.term.reset();
    T.term.write('\x1b[2mConnecting to the workspace…\x1b[0m\r\n');
    send({ type: 'term.open', cols: T.term.cols, rows: T.term.rows, ref: T.ref });
  }
  function closeTerminal() {
    if (T.sid) send({ type: 'term.close', sid: T.sid });
    T.sid = null;
    T.open = false;
    setMax(false);
    renderWorkspace();
  }
  function fitTerminal() {
    if (!T.fit || !T.open) return;
    try { T.fit.fit(); } catch { /* not visible yet */ }
  }
  function setMax(on) {
    T.max = on;
    $('#workspace').classList.toggle('max', on);
    document.body.classList.toggle('term-max', on);
    $('#ws-max').setAttribute('aria-pressed', String(on));
    $('#ws-max').textContent = on ? 'Exit full screen' : 'Full screen';
    requestAnimationFrame(fitTerminal);
  }
  function b64bytes(b64) {
    const bin = atob(b64);
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  function onTerminal(m) {
    if (!T.term) return;
    if (m.type === 'term.opened' && m.ref === T.ref) {
      T.sid = m.sid;
      T.hostId = m.hostId;
      renderWorkspace();
      fitTerminal();
      send({ type: 'term.resize', sid: T.sid, cols: T.term.cols, rows: T.term.rows });
      T.term.focus();
    } else if (m.type === 'term.error' && m.ref === T.ref) {
      T.term.write(`\r\n${m.message}\r\n`);
    } else if (m.type === 'term.out' && m.sid === T.sid) {
      T.term.write(b64bytes(m.data));
    } else if (m.type === 'term.exit' && m.sid === T.sid) {
      T.sid = null;
      T.term.write(`\r\n\x1b[2m${m.message || 'The shell ended.'} Press Open terminal to start again.\x1b[0m\r\n`);
      renderWorkspace();
    }
  }
  function renderWorkspace() {
    const hosts = S.devices.filter((d) => d.workspace);
    const host = hosts.find((h) => h.id === T.hostId) || hosts[0];
    let status;
    if (!host) status = 'No computer is hosting the workspace yet. Add one under "Add a device" with --workspace.';
    else {
      const ws = host.workspace;
      status = `Linux workspace on ${host.name}${ws.shared ? ', one tmux session shared by every device' : `, a ${ws.shell} shell per terminal`}.`;
      if (host.stale) status += ' It isn\'t responding right now.';
    }
    $('#ws-status').textContent = status;
    $('#ws-open').hidden = T.open && !!T.sid;
    $('#ws-open').textContent = T.open ? 'Reconnect' : 'Open terminal';
    $('#ws-open').disabled = !host && !T.open;
    $('#ws-close').hidden = !T.open;
    $('#ws-max').hidden = !T.open;
    $('#term-wrap').hidden = !T.open;
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

  function renderDevices() {
    const list = S.devices.slice().sort((a, b) => (b.id === S.id) - (a.id === S.id) || a.joinedAt - b.joinedAt);
    const sharing = list.filter((d) => d.sharing && !d.stale);
    const threads = sharing.reduce((a, d) => a + d.threads, 0);
    const mem = list.filter((d) => d.info.memoryGB);
    const memText = mem.length ? ` ${gb(mem.reduce((a, d) => a + d.info.memoryGB, 0))} GB of memory reported by ${mem.length === list.length ? 'all of them' : `${mem.length} of them`}.` : '';
    $('#pool-summary').textContent = list.length
      ? `${list.length} ${list.length === 1 ? 'device' : 'devices'} connected, ${sharing.length} sharing ${threads} ${threads === 1 ? 'thread' : 'threads'}.${memText}`
      : 'No devices yet.';

    renderResources();
    const job = viewJob();
    $('#wires').innerHTML = list.map((d) => {
      const share = job && job.total ? ((job.perDevice[d.id]?.chunks || 0) / job.total) * 100 : 0;
      const info = d.info;
      const meta = [
        d.kind === 'agent' ? 'Agent' : 'Browser',
        esc(info.platform),
        d.sharing ? `${d.threads} of ${info.cores} threads` : 'Not sharing',
        info.memoryGB ? `${gb(info.memoryGB)} GB memory${info.memoryFreeGB ? `, ${gb(info.memoryFreeGB)} free` : ''}` : null,
        info.diskFreeGB ? `${gb(info.diskFreeGB)} GB disk free` : null,
        info.gpus && info.gpus.length ? esc(info.gpus.join(', ')) : null,
        d.workspace ? '<strong>Hosts the workspace</strong>' : null,
        d.stale ? '<strong class="warn">Not responding</strong>' : d.busy ? `<strong>Working on ${d.busy}</strong>` : 'Idle',
        `${nf.format(d.done)} pieces done`,
      ].filter(Boolean).map((t) => `<span>${t}</span>`).join('');
      const side = d.id === S.id
        ? '<span class="wire-you">This device</span>'
        : `<button class="btn ghost wire-remove" type="button" data-remove="${esc(d.id)}" data-name="${esc(d.name)}">Remove</button>`;
      return `<li class="wire" style="--wire:${esc(d.color)}" data-stale="${d.stale}">
        <div class="wire-head"><span class="wire-name">${esc(d.name)}</span><span class="wire-side">${side}</span></div>
        <div class="wire-line" data-busy="${d.busy > 0 && !d.stale}" data-sharing="${d.sharing}"><span class="wire-fill" style="width:${share.toFixed(2)}%"></span></div>
        <div class="wire-meta">${meta}</div>
      </li>`;
    }).join('');
  }

  // -------------------------------------------------------------------------
  // Rendering: pooled resources
  // -------------------------------------------------------------------------
  function renderResources() {
    const list = S.devices.slice().sort((a, b) => (b.id === S.id) - (a.id === S.id) || a.joinedAt - b.joinedAt);
    const live = list.filter((d) => d.sharing && !d.stale);
    const threads = live.reduce((a, d) => a + d.threads, 0);
    // A computer running an agent and a browser tab is one machine: count its cores and memory once.
    const byHost = new Map();
    for (const d of live) {
      const key = d.host || d.id;
      const cur = byHost.get(key);
      if (!cur || (d.kind === 'agent' && cur.kind !== 'agent')) byHost.set(key, d);
    }
    const machines = [...byHost.values()];
    const cores = machines.reduce((a, d) => a + d.info.cores, 0);
    const memDevs = machines.filter((d) => d.info.memoryGB);
    const mem = memDevs.reduce((a, d) => a + d.info.memoryGB, 0);
    const memFree = machines.reduce((a, d) => a + (d.info.memoryFreeGB || 0), 0);
    const disk = live.reduce((a, d) => a + (d.info.diskFreeGB || 0), 0);
    const browserStore = live.filter((d) => d.kind !== 'agent').reduce((a, d) => a + (d.info.storageGB || 0), 0);
    const gpuDevs = live.filter((d) => d.info.gpus && d.info.gpus.length).length;
    const tile = (big, label, sub) => `<div class="res-tile"><b>${big}</b><span>${label}</span><span>${sub}</span></div>`;
    $('#res-totals').innerHTML = [
      tile(`${machines.length}`, machines.length === 1 ? 'computer or device sharing' : 'computers and devices sharing', list.length - live.length ? `${list.length - live.length} not sharing` : live.length > machines.length ? 'a browser tab on a computer with an agent counts once' : 'all of them'),
      tile(nf.format(threads), 'threads of compute', `of ${nf.format(cores)} cores on those devices`),
      tile(mem ? `${gb(mem)} GB` : 'None reported', 'memory', mem ? `${gb(memFree)} GB free, reported by ${memDevs.length} of ${machines.length}` : 'iPhone and iPad browsers do not report it'),
      tile(`${gb(disk)} GB`, 'free disk on computers', browserStore ? `plus up to ${gb(browserStore)} GB of browser storage` : 'agents report their disk'),
      tile(nf.format(gpuDevs), gpuDevs === 1 ? 'device with graphics' : 'devices with graphics', 'WebGPU in browsers, Metal or CUDA on computers'),
    ].join('');

    const na = (t) => `<span class="res-na">${t}</span>`;
    $('#res-table tbody').innerHTML = list.map((d) => {
      const i = d.info;
      const off = !d.sharing || d.stale;
      const pct = i.cores ? Math.min(100, (d.threads / i.cores) * 100) : 0;
      const memCell = i.memoryGB
        ? `${gb(i.memoryGB)} GB${i.memoryFreeGB ? `<span class="res-sub">${gb(i.memoryFreeGB)} GB free</span><span class="res-bar"><i style="width:${Math.min(100, (i.memoryFreeGB / i.memoryGB) * 100).toFixed(0)}%"></i></span>` : ''}`
        : na(d.kind === 'agent' ? 'Not reported' : `Not reported by ${esc(i.platform) === 'iPhone' || esc(i.platform) === 'iPad' ? 'Safari' : 'this browser'}`);
      const diskCell = d.kind === 'agent'
        ? (i.diskFreeGB ? `${gb(i.diskFreeGB)} GB free` : na('Not reported'))
        : (i.storageGB ? `up to ${gb(i.storageGB)} GB<span class="res-sub">browser storage</span>` : na('Not reported'));
      const gpuCell = i.gpus && i.gpus.length ? esc(i.gpus.join(', ')) : na('None');
      const status = d.stale ? 'Not responding' : !d.sharing ? 'Not sharing' : null;
      return `<tr style="--wire:${esc(d.color)}" data-off="${off}">
        <td><span class="res-name">${esc(d.name)}</span><span class="res-kind">${d.kind === 'agent' ? 'Agent' : 'Browser'}, ${esc(i.platform)}${status ? `, ${status}` : ''}</span></td>
        <td>${d.threads} of ${i.cores} threads<span class="res-bar"><i style="width:${pct.toFixed(0)}%"></i></span></td>
        <td>${memCell}</td><td>${diskCell}</td><td>${gpuCell}</td></tr>`;
    }).join('');
  }

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
    $('#file-list').innerHTML = S.files.length
      ? S.files.map((f) => `<li>
          <span class="file-name">${esc(f.name)}</span>
          <span class="file-meta">${fmtSize(f.size)}${f.by ? `, added by ${esc(f.by)}` : ''}</span>
          <span class="file-actions">
            <button class="btn ghost small" type="button" data-search="${esc(f.name)}">Search</button>
            <a class="btn ghost small" href="/api/files/${encodeURIComponent(f.name)}" download="${esc(f.name)}">Download</a>
            <button class="btn ghost small danger" type="button" data-delete="${esc(f.name)}" aria-label="Delete ${esc(f.name)}">Delete</button>
          </span>
        </li>`).join('')
      : '<li class="empty">No shared files yet. Files added from any device can be read by every device.</li>';

    const sel = $('#g-file');
    const current = sel.value;
    sel.innerHTML = S.files.map((f) => `<option value="${esc(f.name)}">${esc(f.name)} (${fmtSize(f.size)})</option>`).join('');
    if (S.files.some((f) => f.name === current)) sel.value = current;
    $('#g-empty').hidden = S.files.length > 0;
  }

  function renderPool() {
    const p = S.pool;
    $('#pool-info').textContent = p.agents
      ? `Spread over ${p.agents} ${p.agents === 1 ? 'computer' : 'computers'}, about ${nf.format(p.freeGB)} GB free. ${fmtSize(p.storedBytes)} stored. Each file is kept on ${Math.min(p.replicas, p.agents)} ${Math.min(p.replicas, p.agents) === 1 ? 'computer' : 'different computers'}.`
      : 'No computer is connected to hold files. Add a Mac, Linux or Windows computer under "Add a device". Phones and tablets can add and read files but do not hold them.';
    $('#pool-list').innerHTML = p.files.length
      ? p.files.map((f) => {
        const state = !f.available ? 'Offline: a computer holding it is away'
          : f.copies < Math.min(f.wanted, p.agents) ? `${f.copies} of ${Math.min(f.wanted, p.agents)} copies, repairing` : `${f.copies} ${f.copies === 1 ? 'copy' : 'copies'}`;
        return `<li>
          <span class="file-name">${esc(f.name)}</span>
          <span class="file-meta">${fmtSize(f.size)}, ${esc(state)}${f.by ? `, added by ${esc(f.by)}` : ''}</span>
          <span class="file-actions">
            ${f.available ? `<a class="btn ghost small" href="/api/pool/${encodeURIComponent(f.name)}" download="${esc(f.name)}">Download</a>` : ''}
            <button class="btn ghost small danger" type="button" data-pool-delete="${esc(f.name)}" aria-label="Delete ${esc(f.name)}">Delete</button>
          </span>
        </li>`;
      }).join('')
      : '<li class="empty">Nothing here yet. Files added here are cut into pieces and spread over the computers in the pool.</li>';
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
    const link = `${location.origin}/#join=${S.joinToken}`;
    $('#add-code').value = S.joinCode || '';
    $('#add-link').value = link;
    $('#add-cmd').value = `curl -fsSL ${location.origin}/install.sh | bash -s -- --join ${S.joinCode} --workspace`;
    $('#add-win').value = `irm ${location.origin}/install.ps1 | iex`;
  }

  function renderAll() {
    renderDevices();
    renderMe();
    renderCurrent(true);
    renderHistory();
    renderFiles();
    renderPool();
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
    $('#app').hidden = false;
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

  $('#wires').addEventListener('click', async (e) => {
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
  $('#ws-open').addEventListener('click', openTerminal);
  $('#ws-close').addEventListener('click', closeTerminal);
  $('#ws-max').addEventListener('click', () => setMax(!T.max));
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && T.max && e.target === document.body) setMax(false); });
  document.querySelector('.keybar').addEventListener('click', (e) => {
    const b = e.target.closest('button[data-seq]');
    if (!b || !T.sid) return;
    send({ type: 'term.in', sid: T.sid, data: KEYS[b.dataset.seq] });
    T.term.focus();
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
  $('#file-input').addEventListener('change', async (e) => {
    const files = [...e.target.files];
    e.target.value = '';
    const status = $('#upload-status');
    for (const f of files) {
      try {
        await uploadFile(f, (pct) => { status.textContent = `Adding ${f.name}: ${pct}%`; });
        status.textContent = `Added ${f.name}.`;
      } catch (err) {
        status.textContent = `Could not add ${f.name}. ${err.message}`;
      }
    }
  });
  function uploadFile(file, onPct, base = '/api/files') {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('PUT', `${base}/${encodeURIComponent(file.name)}`);
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
  $('#pool-input').addEventListener('change', async (e) => {
    const files = [...e.target.files];
    e.target.value = '';
    const status = $('#pool-status');
    for (const f of files) {
      try {
        await uploadFile(f, (pct) => { status.textContent = `Adding ${f.name}: ${pct}%`; }, '/api/pool');
        status.textContent = `Added ${f.name}.`;
      } catch (err) {
        status.textContent = `Could not add ${f.name}. ${err.message}`;
      }
    }
  });
  $('#pool-list').addEventListener('click', async (e) => {
    const del = e.target.closest('button[data-pool-delete]');
    if (!del) return;
    const name = del.dataset.poolDelete;
    if (!confirm(`Delete ${name} from pooled storage? This removes every copy.`)) return;
    const res = await fetch(`/api/pool/${encodeURIComponent(name)}`, { method: 'DELETE', credentials: 'same-origin' });
    if (!res.ok) $('#pool-status').textContent = `Could not delete ${name}.`;
  });
  $('#file-list').addEventListener('click', async (e) => {
    const del = e.target.closest('button[data-delete]');
    const search = e.target.closest('button[data-search]');
    if (del) {
      const name = del.dataset.delete;
      if (!confirm(`Delete ${name} from the shared folder? This removes it for every device.`)) return;
      const res = await fetch(`/api/files/${encodeURIComponent(name)}`, { method: 'DELETE', credentials: 'same-origin' });
      if (!res.ok) $('#upload-status').textContent = `Could not delete ${name}.`;
    } else if (search) {
      $('input[name="kind"][value="grep"]').checked = true;
      showKind();
      $('#g-file').value = search.dataset.search;
      $('#run').scrollIntoView({ behavior: reduceMotion() ? 'auto' : 'smooth', block: 'start' });
      $('#g-term').focus({ preventScroll: true });
    }
  });

  // -------------------------------------------------------------------------
  // Start: a join link in the address bar signs this device in; otherwise try
  // the cookie from an earlier visit.
  // -------------------------------------------------------------------------
  showKind();
  const fromLink = parseJoin(location.hash);
  if (fromLink) {
    history.replaceState(null, '', location.pathname);
    joinWith(fromLink, S.name)
      .then(() => { S.joined = true; connect(); })
      .catch((err) => showJoin(err.message));
  } else {
    S.joined = true;
    connect();
  }
})();
