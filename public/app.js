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

  const CORES = Math.max(1, navigator.hardwareConcurrency || 4);
  const MAX_THREADS = Math.min(32, Math.max(4, CORES * 2));
  const PLATFORM = detectPlatform();

  const FRACTAL_PLACES = {
    elephant: { cx: 0.285, cy: 0.0115, span: 0.012 },
    spiral: { cx: -0.761574, cy: -0.0847596, span: 0.0035 },
    seahorse: { cx: -0.7453, cy: 0.1127, span: 0.0065 },
    whole: { cx: -0.6, cy: 0, span: 3.2 },
  };

  const DEFAULT_CODE = `// Runs once per piece, in a background thread on every sharing device.
// chunk goes from 0 to total - 1. Return a number, or anything JSON can hold.
function run(chunk, total) {
  const n = 2e9;
  let sum = 0;
  for (let i = chunk; i < n; i += total) sum += Math.sqrt(i);
  return sum;
}
`;

  const S = {
    pin: '',
    name: store.get('name') || defaultName(),
    clientId: store.get('clientId') || newId(),
    threads: clampThreads(Number(store.get('threads')) || Math.max(1, CORES - 1)),
    sharing: store.get('sharing') !== 'off',
    id: null,
    color: null,
    devices: [],
    jobs: [],
    files: [],
    view: null, // { jobId, colors: [], values: [] | null }
    follow: true,
    ws: null,
    retry: 0,
    joined: false,
  };
  store.set('clientId', S.clientId);

  // A PIN in the link (from the server's QR code) joins without typing.
  const params = new URLSearchParams(location.search);
  S.pin = params.get('pin') || store.get('pin') || '';
  if (params.has('pin')) history.replaceState(null, '', location.pathname);

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
  function deviceInfo() {
    return {
      platform: PLATFORM,
      cores: CORES,
      memoryGB: typeof navigator.deviceMemory === 'number' ? navigator.deviceMemory : null,
      gpu: 'gpu' in navigator,
    };
  }

  // -------------------------------------------------------------------------
  // Connection
  // -------------------------------------------------------------------------
  function send(msg) {
    if (S.ws && S.ws.readyState === 1) S.ws.send(JSON.stringify(msg));
  }

  function connect() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${proto}//${location.host}/ws`);
    S.ws = ws;
    setConn('connecting');
    ws.onopen = () => {
      S.retry = 0;
      ws.send(JSON.stringify({
        type: 'hello', pin: S.pin, clientId: S.clientId, name: S.name,
        threads: S.threads, sharing: S.sharing, info: deviceInfo(),
      }));
    };
    ws.onmessage = (e) => {
      let m;
      try { m = JSON.parse(e.data); } catch { return; }
      handle(m);
    };
    ws.onclose = (e) => {
      if (S.ws !== ws) return;
      setConn('offline');
      if (e.code === 4003) return; // wrong PIN, wait for the person
      const delay = Math.min(5000, 400 * 2 ** S.retry++);
      setTimeout(() => { if (S.ws === ws) connect(); }, delay);
    };
  }

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible' && S.joined && (!S.ws || S.ws.readyState > 1)) {
      S.retry = 0;
      connect();
    }
    updateWakeLock();
  });

  function handle(m) {
    switch (m.type) {
      case 'welcome':
        S.id = m.id;
        S.color = m.color;
        S.joined = true;
        store.set('pin', S.pin);
        store.set('name', S.name);
        S.devices = m.devices;
        S.jobs = m.jobs;
        showApp();
        setConn('online');
        if (S.follow || !S.view) {
          if (m.snapshot) setView(m.snapshot);
        }
        renderAll();
        break;
      case 'denied':
        S.ws = null;
        S.pin = '';
        store.del('pin');
        showJoin(m.reason);
        break;
      case 'devices':
        S.devices = m.devices;
        renderDevices();
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
      case 'chunk':
        pool.queue.push(m);
        pump();
        break;
      case 'abort':
        dropJob(m.jobId);
        break;
      case 'files':
        S.files = m.files;
        renderFiles();
        break;
      case 'error':
        $('#run-error').textContent = m.message;
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
      s.w.postMessage({ task: t, pin: S.pin });
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
    pool.queue = pool.queue.filter((t) => t.jobId !== jobId);
    pool.slots.forEach((s, i) => {
      if (!s.task || s.task.jobId !== jobId) return;
      s.w.terminate(); // stops long-running pieces right away
      if (s.retire) pool.slots[i] = null;
      else pool.slots[i] = makeSlot();
    });
    pool.slots = pool.slots.filter(Boolean);
    pump();
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
    else if (['iPhone', 'iPad', 'Android'].includes(PLATFORM)) el.textContent = 'Keep this tab open and the screen on. Phones and tablets pause background tabs, and their pieces move to other devices.';
    else el.textContent = 'Keep this tab open. Background tabs can be slowed down by the browser.';
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
  function renderDevices() {
    const list = S.devices.slice().sort((a, b) => (b.id === S.id) - (a.id === S.id) || a.joinedAt - b.joinedAt);
    const sharing = list.filter((d) => d.sharing);
    const threads = sharing.reduce((a, d) => a + d.threads, 0);
    const mem = list.filter((d) => d.info.memoryGB);
    const memText = mem.length ? ` ${nf.format(mem.reduce((a, d) => a + d.info.memoryGB, 0))} GB of memory reported by ${mem.length === list.length ? 'all of them' : `${mem.length} of them`}.` : '';
    $('#pool-summary').textContent = list.length
      ? `${list.length} ${list.length === 1 ? 'device' : 'devices'} connected, ${sharing.length} sharing ${threads} ${threads === 1 ? 'thread' : 'threads'}.${memText}`
      : 'No devices yet.';

    const job = viewJob();
    $('#wires').innerHTML = list.map((d) => {
      const share = job && job.total ? ((job.perDevice[d.id]?.chunks || 0) / job.total) * 100 : 0;
      const meta = [
        esc(d.info.platform),
        d.sharing ? `${d.threads} of ${d.info.cores} threads` : 'Not sharing',
        d.info.memoryGB ? `${nf.format(d.info.memoryGB)} GB memory` : null,
        d.busy ? `<strong>Working on ${d.busy}</strong>` : 'Idle',
        `${nf.format(d.done)} pieces done`,
      ].filter(Boolean).map((t) => `<span>${t}</span>`).join('');
      return `<li class="wire" style="--wire:${esc(d.color)}">
        <div class="wire-head"><span class="wire-name">${esc(d.name)}</span>${d.id === S.id ? '<span class="wire-you">This device</span>' : ''}</div>
        <div class="wire-line" data-busy="${d.busy > 0}" data-sharing="${d.sharing}"><span class="wire-fill" style="width:${share.toFixed(2)}%"></span></div>
        <div class="wire-meta">${meta}</div>
      </li>`;
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
  const sharingNow = () => S.devices.some((d) => d.sharing);

  function statusText(j) {
    switch (j.status) {
      case 'queued': return 'Waiting for the run ahead of it to finish.';
      case 'running': return `${nf.format(j.done)} of ${nf.format(j.total)} pieces done in ${fmtDur(elapsed(j))}.${sharingNow() ? '' : ' No device is sharing right now, so nothing is being computed.'}`;
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
      case 'custom': {
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
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    const iters = new Uint16Array(bytes.buffer);
    const { width, rows, lut, mark, ctx } = fractal;
    const h = iters.length / width;
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
      const j = viewJob();
      if (!j) return;
      layoutMap(j.total);
      for (let i = 0; i < j.total; i++) drawCell(i);
    }, 150);
  });

  // -------------------------------------------------------------------------
  // Rendering: history, files, this device
  // -------------------------------------------------------------------------
  function renderHistory() {
    const list = S.jobs.slice().reverse();
    const stateText = { queued: 'Waiting', running: 'Running', done: 'Done', failed: 'Stopped', cancelled: 'Cancelled' };
    $('#history').innerHTML = list.length
      ? list.map((j) => `<li><button type="button" data-job="${esc(j.id)}" aria-current="${S.view && S.view.jobId === j.id}">
          <span class="h-title">${esc(j.title)}</span>
          <span class="h-when">Started by ${esc(j.createdBy)}${j.startedAt && j.finishedAt ? `, took ${fmtDur(j.finishedAt - j.startedAt)}` : ''}</span>
          <span class="h-state">${stateText[j.status] || j.status}</span>
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
  function fileUrl(name, download) {
    return `/api/files/${encodeURIComponent(name)}?pin=${encodeURIComponent(S.pin)}${download ? '&download=1' : ''}`;
  }
  function renderFiles() {
    $('#file-list').innerHTML = S.files.length
      ? S.files.map((f) => `<li>
          <span class="file-name">${esc(f.name)}</span>
          <span class="file-meta">${fmtSize(f.size)}${f.by ? `, added by ${esc(f.by)}` : ''}</span>
          <span class="file-actions">
            <button class="btn ghost small" type="button" data-search="${esc(f.name)}">Search</button>
            <a class="btn ghost small" href="${fileUrl(f.name, true)}" download="${esc(f.name)}">Download</a>
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

  function renderAll() {
    renderDevices();
    renderMe();
    renderCurrent(true);
    renderHistory();
    renderFiles();
  }

  // -------------------------------------------------------------------------
  // Screens
  // -------------------------------------------------------------------------
  function showJoin(error) {
    $('#app').hidden = true;
    $('#join').hidden = false;
    $('#join-name').value = S.name;
    $('#join-pin').value = '';
    $('#join-error').textContent = error || '';
    (S.name ? $('#join-pin') : $('#join-name')).focus();
  }
  function revealShell() {
    $('#join').hidden = true;
    $('#app').hidden = false;
  }
  function showApp() {
    revealShell();
    resizePool(S.sharing ? S.threads : 0);
    updateWakeLock();
  }

  // -------------------------------------------------------------------------
  // Events
  // -------------------------------------------------------------------------
  $('#join-form').addEventListener('submit', (e) => {
    e.preventDefault();
    const name = $('#join-name').value.trim();
    const pin = $('#join-pin').value.trim();
    if (!name) { $('#join-error').textContent = 'Give this device a name so you can tell it apart.'; return; }
    if (!pin) { $('#join-error').textContent = 'Enter the PIN from the server.'; return; }
    S.name = name;
    S.pin = pin;
    $('#join-error').textContent = '';
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
  $('#me-leave').addEventListener('click', () => {
    store.del('pin');
    S.pin = '';
    S.joined = false;
    const ws = S.ws;
    S.ws = null;
    if (ws) ws.close();
    resizePool(0);
    pool.queue = [];
    updateWakeLock();
    showJoin('You left the pool. Enter the PIN to join again.');
  });

  // Job type switching
  document.querySelectorAll('input[name="kind"]').forEach((r) => r.addEventListener('change', showKind));
  function selectedKind() { return $('input[name="kind"]:checked').value; }
  function showKind() {
    const kind = selectedKind();
    document.querySelectorAll('#run-form fieldset').forEach((f) => { f.hidden = f.dataset.kind !== kind; });
    $('#run-error').textContent = '';
  }
  $('#c-code').value = DEFAULT_CODE;

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
        return { code: $('#c-code').value, chunks: Number($('#c-chunks').value), combine: $('#c-combine').value };
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
    if (kind === 'custom' && !/function\s+run\s*\(|run\s*=/.test($('#c-code').value)) { err.textContent = 'The code needs a function named run(chunk, total).'; return; }
    if (!S.ws || S.ws.readyState !== 1) { err.textContent = 'Not connected to the server. Waiting to reconnect.'; return; }
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
    $('#current').scrollIntoView({ behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' });
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
  function uploadFile(file, onPct) {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('PUT', `/api/files/${encodeURIComponent(file.name)}`);
      xhr.setRequestHeader('x-harness-pin', S.pin);
      xhr.setRequestHeader('x-harness-device', S.id || '');
      xhr.upload.onprogress = (ev) => { if (ev.lengthComputable) onPct(Math.round((ev.loaded / ev.total) * 100)); };
      xhr.onload = () => {
        if (xhr.status < 300) return resolve();
        let msg = `The server answered ${xhr.status}.`;
        try { msg = JSON.parse(xhr.responseText).error || msg; } catch { /* not JSON */ }
        reject(new Error(msg));
      };
      xhr.onerror = () => reject(new Error('The connection dropped.'));
      xhr.send(file);
    });
  }
  $('#file-list').addEventListener('click', async (e) => {
    const del = e.target.closest('button[data-delete]');
    const search = e.target.closest('button[data-search]');
    if (del) {
      const name = del.dataset.delete;
      if (!confirm(`Delete ${name} from the shared folder? This removes it for every device.`)) return;
      const res = await fetch(`/api/files/${encodeURIComponent(name)}`, { method: 'DELETE', headers: { 'x-harness-pin': S.pin } });
      if (!res.ok) $('#upload-status').textContent = `Could not delete ${name}.`;
    } else if (search) {
      $('input[name="kind"][value="grep"]').checked = true;
      showKind();
      $('#g-file').value = search.dataset.search;
      $('#run').scrollIntoView({ behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' });
      $('#g-term').focus({ preventScroll: true });
    }
  });

  // -------------------------------------------------------------------------
  // Start
  // -------------------------------------------------------------------------
  showKind();
  if (S.pin) {
    revealShell();
    renderMe();
    renderCurrent(false);
    renderHistory();
    renderFiles();
    connect();
  } else {
    showJoin();
  }
})();
