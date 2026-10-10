#!/usr/bin/env node
// YourHarness server.
// Serves the portal, keeps track of every device that joins, splits jobs into
// pieces, hands pieces to whichever device has a free thread, and hosts the
// shared folder that every device can read and write.

import http from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import qrcode from 'qrcode-terminal';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Configuration (all optional, set through environment variables)
// ---------------------------------------------------------------------------
const PORT = process.env.PORT === undefined ? 8420 : Number(process.env.PORT);
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');
const SHARED_DIR = path.resolve(process.env.SHARED_DIR || path.join(__dirname, 'shared'));
const PIN = String(process.env.HARNESS_PIN || crypto.randomInt(100000, 1000000)).trim();
const MAX_UPLOAD_BYTES = Number(process.env.MAX_UPLOAD_MB || 4096) * 1024 * 1024;
const CHUNK_TIMEOUT_MS = Number(process.env.CHUNK_TIMEOUT_S || 300) * 1000;
const JOB_HISTORY = 20;
const WIRE_COLORS = ['#E0A100', '#2563C9', '#D1433B', '#23935A', '#8B5CC4', '#E0742A', '#1795A8', '#C2417D'];

fs.mkdirSync(SHARED_DIR, { recursive: true });

class UserError extends Error {}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------
function pinOk(value) {
  if (typeof value !== 'string') return false;
  const a = Buffer.from(value.trim());
  const b = Buffer.from(PIN);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

const clampInt = (v, lo, hi, dflt) => {
  const n = Math.floor(Number(v));
  return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : dflt;
};
const clampNum = (v, lo, hi, dflt) => {
  const n = Number(v);
  return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : dflt;
};
const fmtInt = (n) => Number(n).toLocaleString('en-US');
const sum = (arr) => arr.reduce((a, b) => a + (Number(b) || 0), 0);

function sendJson(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(data), 'Cache-Control': 'no-store' });
  res.end(data);
}
function sendText(res, status, text) {
  res.writeHead(status, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(text);
}

// File names arrive URL-encoded in the path. Decode, then keep them flat and visible.
function safeName(raw) {
  let name;
  try { name = decodeURIComponent(raw); } catch { return null; }
  return sanitizeName(name);
}
function sanitizeName(name) {
  const clean = String(name).replace(/[\\/]/g, '_').replace(/[\x00-\x1f\x7f]/g, '').trim();
  if (!clean || clean.startsWith('.') || clean.length > 200) return null;
  return clean;
}

// ---------------------------------------------------------------------------
// Job types. The server plans and combines; the devices compute (public/jobs.js).
// ---------------------------------------------------------------------------
const JOB_KINDS = {
  mandelbrot: {
    plan(p) {
      const width = clampInt(p.width, 64, 4096, 1600);
      const height = clampInt(p.height, 64, 4096, 1000);
      const maxIter = clampInt(p.maxIter, 50, 60000, 3000);
      const cx = clampNum(p.cx, -3, 3, -0.743643887037151);
      const cy = clampNum(p.cy, -3, 3, 0.13182590420533);
      const span = clampNum(p.span, 1e-13, 6, 0.006);
      const rows = clampInt(p.rows, 1, 64, 8);
      return {
        title: `Fractal, ${width} × ${height} at ${fmtInt(maxIter)} iterations`,
        total: Math.ceil(height / rows),
        params: { width, height, maxIter, cx, cy, span, rows },
      };
    },
    check: (v) => typeof v === 'string',
    reduce: (job) => ({ pixels: job.params.width * job.params.height }),
  },
  primes: {
    plan(p) {
      const limit = clampInt(p.limit, 1000, 1e11, 5e9);
      const chunkSize = clampInt(p.chunkSize, 1e5, 1e8, 1e7);
      const total = Math.ceil(limit / chunkSize);
      if (total > 20000) throw new UserError('That range splits into too many pieces. Use a larger piece size.');
      return { title: `Count primes below ${fmtInt(limit)}`, total, params: { limit, chunkSize } };
    },
    check: (v) => Number.isFinite(v),
    reduce: (job) => ({ count: sum(job.results) }),
  },
  pi: {
    plan(p) {
      const chunks = clampInt(p.chunks, 1, 20000, 400);
      const samples = clampInt(p.samples, 1e4, 1e9, 2e7);
      return { title: `Estimate π from ${fmtInt(chunks * samples)} random points`, total: chunks, params: { chunks, samples } };
    },
    check: (v) => Number.isFinite(v),
    reduce: (job) => {
      const n = job.params.chunks * job.params.samples;
      return { estimate: (4 * sum(job.results)) / n, samples: n };
    },
  },
  grep: {
    async plan(p) {
      const file = sanitizeName(String(p.file ?? ''));
      if (!file) throw new UserError('Pick a shared file to search.');
      const term = String(p.term ?? '');
      if (!term || term.length > 200) throw new UserError('Enter the text to find, up to 200 characters.');
      let st;
      try { st = await fsp.stat(path.join(SHARED_DIR, file)); } catch { throw new UserError(`There is no shared file named ${file}.`); }
      if (!st.isFile() || st.size === 0) throw new UserError(`${file} is empty, so there is nothing to search.`);
      const chunkBytes = clampInt(p.chunkMB, 1, 256, 4) * 1024 * 1024;
      return {
        title: `Search ${file} for “${term}”`,
        total: Math.ceil(st.size / chunkBytes),
        params: { file, term, ignoreCase: Boolean(p.ignoreCase), chunkBytes, size: st.size },
      };
    },
    check: (v) => Number.isFinite(v),
    reduce: (job) => ({ count: sum(job.results) }),
  },
  custom: {
    plan(p) {
      const code = String(p.code ?? '');
      if (!code.trim()) throw new UserError('Write a run(chunk, total) function first.');
      if (code.length > 100_000) throw new UserError('The code is over 100 KB. Load big inputs from a shared file instead.');
      const chunks = clampInt(p.chunks, 1, 20000, 200);
      const combine = ['sum', 'max', 'min', 'list'].includes(p.combine) ? p.combine : 'sum';
      return { title: `Your code, split into ${fmtInt(chunks)} pieces`, total: chunks, params: { code, chunks, combine } };
    },
    check: () => true,
    reduce: (job) => {
      const { combine } = job.params;
      if (combine === 'list') return { value: job.results };
      const nums = job.results.map(Number);
      if (nums.some((n) => !Number.isFinite(n))) {
        throw new UserError(`Every piece has to return a number to combine them that way. Choose "Keep as a list" to see raw results.`);
      }
      const value = combine === 'sum' ? nums.reduce((a, b) => a + b, 0)
        : combine === 'max' ? nums.reduce((a, b) => Math.max(a, b), -Infinity)
        : nums.reduce((a, b) => Math.min(a, b), Infinity);
      return { value };
    },
  },
};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
const devices = new Map(); // id -> device
const colorMemory = new Map(); // client id -> wire color, so a device keeps its color across reconnects
const fileMeta = new Map(); // file name -> { by, color }
const jobs = []; // oldest first
let jobSeq = 0;

const chunkKey = (jobId, idx) => `${jobId}:${idx}`;
const findJob = (id) => jobs.find((j) => j.id === id);
const runningJob = () => jobs.find((j) => j.status === 'running');

function send(ws, msg) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
}
function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const d of devices.values()) if (d.ws.readyState === 1) d.ws.send(data);
}

function deviceView(d) {
  return {
    id: d.id, name: d.name, color: d.color, info: d.info, threads: d.threads, sharing: d.sharing,
    busy: d.inflight.size, done: d.done, busyMs: d.busyMs, joinedAt: d.joinedAt,
  };
}
const deviceList = () => [...devices.values()].map(deviceView);

let devicesTimer = null;
function broadcastDevices() {
  if (devicesTimer) return;
  devicesTimer = setTimeout(() => {
    devicesTimer = null;
    broadcast({ type: 'devices', devices: deviceList() });
  }, 250);
}

function jobSummary(j) {
  return {
    id: j.id, kind: j.kind, title: j.title, params: j.params, total: j.total, done: j.doneCount,
    status: j.status, perDevice: j.perDevice, createdBy: j.createdBy, createdAt: j.createdAt,
    startedAt: j.startedAt, finishedAt: j.finishedAt, result: j.result, error: j.error,
  };
}
function snapshot(j) {
  if (!j) return null;
  return {
    job: jobSummary(j),
    colors: j.by.map((id) => (id ? j.perDevice[id]?.color ?? null : null)),
    values: j.kind === 'mandelbrot' ? j.results : null,
  };
}

function pickColor(clientId) {
  const remembered = colorMemory.get(clientId);
  const inUse = new Set([...devices.values()].map((d) => d.color));
  if (remembered && !inUse.has(remembered)) return remembered;
  const free = WIRE_COLORS.find((c) => !inUse.has(c));
  return free || WIRE_COLORS[devices.size % WIRE_COLORS.length];
}

function cleanText(v, max, fallback) {
  const s = String(v ?? '').replace(/[\x00-\x1f\x7f]/g, '').trim().slice(0, max);
  return s || fallback;
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------
// Pull-based: a device gets a new piece whenever it has a free thread, so fast
// devices naturally take more pieces and slow ones are never a bottleneck.
function dispatch() {
  const job = runningJob();
  if (!job) return;
  let gave = true;
  while (gave && job.pending.length) {
    gave = false;
    for (const d of devices.values()) {
      if (!job.pending.length) break;
      if (!d.sharing || d.ws.readyState !== 1) continue;
      if (d.threads - d.inflight.size <= 0) continue;
      const idx = job.pending.shift();
      job.inflight.set(idx, { deviceId: d.id, t: Date.now() });
      d.inflight.add(chunkKey(job.id, idx));
      send(d.ws, { type: 'chunk', jobId: job.id, kind: job.kind, params: job.params, idx, total: job.total });
      gave = true;
    }
  }
  broadcastDevices();
}

function requeueDevice(device) {
  for (const key of device.inflight) {
    const sep = key.lastIndexOf(':');
    const job = findJob(key.slice(0, sep));
    const idx = Number(key.slice(sep + 1));
    if (!job || job.status !== 'running') continue;
    if (job.inflight.get(idx)?.deviceId === device.id) {
      job.inflight.delete(idx);
      if (job.by[idx] === null && !job.pending.includes(idx)) job.pending.unshift(idx);
    }
  }
  device.inflight.clear();
}

function clearJobFromDevices(job) {
  for (const d of devices.values()) {
    for (const key of [...d.inflight]) if (key.startsWith(`${job.id}:`)) d.inflight.delete(key);
  }
}

async function createJob(device, spec) {
  const kind = spec?.kind;
  const def = JOB_KINDS[kind];
  if (!def) throw new UserError('Unknown job type.');
  const plan = await def.plan(spec.params || {});
  const job = {
    id: `j${++jobSeq}`, kind, title: plan.title, params: plan.params, total: plan.total,
    pending: Array.from({ length: plan.total }, (_, i) => i),
    inflight: new Map(),
    results: new Array(plan.total).fill(null),
    by: new Array(plan.total).fill(null),
    attempts: new Array(plan.total).fill(0),
    doneCount: 0, perDevice: {}, status: 'queued',
    createdBy: device.name, createdAt: Date.now(), startedAt: null, finishedAt: null,
    result: null, error: null,
  };
  jobs.push(job);
  // Keep a short history. Never drop a job that is still queued or running.
  while (jobs.length > JOB_HISTORY) {
    const old = jobs.findIndex((j) => j.status !== 'running' && j.status !== 'queued');
    if (old < 0) break;
    jobs.splice(old, 1);
  }
  broadcast({ type: 'job', job: jobSummary(job) });
  startNext();
}

function startNext() {
  if (runningJob()) return;
  const next = jobs.find((j) => j.status === 'queued');
  if (!next) return;
  next.status = 'running';
  next.startedAt = Date.now();
  broadcast({ type: 'job', job: jobSummary(next) });
  dispatch();
}

function finishJob(job) {
  try {
    job.result = JOB_KINDS[job.kind].reduce(job);
    job.status = 'done';
  } catch (e) {
    job.status = 'failed';
    job.error = e instanceof UserError ? e.message : `Could not combine the results: ${e.message}`;
  }
  job.finishedAt = Date.now();
  job.pending = [];
  job.inflight.clear();
  clearJobFromDevices(job);
  broadcast({ type: 'job', job: jobSummary(job) });
  broadcastDevices();
  startNext();
}

function stopJob(job, status, error) {
  if (job.status !== 'running' && job.status !== 'queued') return;
  const wasRunning = job.status === 'running';
  job.status = status;
  job.error = error || null;
  job.finishedAt = Date.now();
  job.pending = [];
  job.inflight.clear();
  clearJobFromDevices(job);
  if (wasRunning) broadcast({ type: 'abort', jobId: job.id });
  broadcast({ type: 'job', job: jobSummary(job) });
  broadcastDevices();
  startNext();
}

function acceptResult(device, msg) {
  const { jobId, idx } = msg;
  device.inflight.delete(chunkKey(jobId, idx));
  const job = findJob(jobId);
  if (!job || job.status !== 'running' || !Number.isInteger(idx) || idx < 0 || idx >= job.total) { dispatch(); return; }
  if (job.by[idx] !== null) { dispatch(); return; } // someone already finished this piece
  if (!JOB_KINDS[job.kind].check(msg.value)) {
    return chunkFailed(device, { jobId, idx, error: 'Returned a result in the wrong format.' });
  }
  const holder = job.inflight.get(idx);
  if (holder && holder.deviceId !== device.id) devices.get(holder.deviceId)?.inflight.delete(chunkKey(jobId, idx));
  job.inflight.delete(idx);
  const p = job.pending.indexOf(idx);
  if (p >= 0) job.pending.splice(p, 1);

  const ms = clampNum(msg.ms, 0, 1e9, 0);
  job.results[idx] = msg.value;
  job.by[idx] = device.id;
  job.doneCount += 1;
  const pd = (job.perDevice[device.id] ||= { name: device.name, color: device.color, chunks: 0, ms: 0 });
  pd.chunks += 1;
  pd.ms += ms;
  device.done += 1;
  device.busyMs += ms;

  broadcast({
    type: 'progress', jobId, idx, deviceId: device.id, deviceName: device.name, color: device.color, ms,
    done: job.doneCount, total: job.total, value: job.kind === 'mandelbrot' ? msg.value : undefined,
  });

  if (job.doneCount === job.total) finishJob(job);
  else dispatch();
}

function chunkFailed(device, msg) {
  const { jobId, idx } = msg;
  device.inflight.delete(chunkKey(jobId, idx));
  const job = findJob(jobId);
  if (!job || job.status !== 'running' || !Number.isInteger(idx) || job.by[idx] !== null) { dispatch(); return; }
  if (job.inflight.get(idx)?.deviceId === device.id) job.inflight.delete(idx);
  job.attempts[idx] += 1;
  const reason = cleanText(msg.error, 500, 'Unknown error');
  // Custom code that throws will throw again elsewhere, so stop right away.
  if (job.kind === 'custom' || job.attempts[idx] >= 3) {
    stopJob(job, 'failed', `Piece ${idx + 1} failed on ${device.name}: ${reason}`);
    return;
  }
  if (!job.pending.includes(idx)) job.pending.unshift(idx);
  dispatch();
}

// Pieces that take far too long (a tab that froze) go back in the queue.
setInterval(() => {
  const job = runningJob();
  if (!job) return;
  const now = Date.now();
  let moved = false;
  for (const [idx, h] of job.inflight) {
    if (now - h.t > CHUNK_TIMEOUT_MS) {
      job.inflight.delete(idx);
      devices.get(h.deviceId)?.inflight.delete(chunkKey(job.id, idx));
      if (!job.pending.includes(idx)) job.pending.push(idx);
      moved = true;
    }
  }
  if (moved) dispatch();
}, 5000).unref();

// ---------------------------------------------------------------------------
// Shared folder
// ---------------------------------------------------------------------------
async function listFiles() {
  const entries = await fsp.readdir(SHARED_DIR, { withFileTypes: true });
  const out = [];
  for (const e of entries) {
    if (!e.isFile() || e.name.startsWith('.')) continue;
    const st = await fsp.stat(path.join(SHARED_DIR, e.name)).catch(() => null);
    if (!st) continue;
    const meta = fileMeta.get(e.name) || {};
    out.push({ name: e.name, size: st.size, mtime: st.mtimeMs, by: meta.by || null, color: meta.color || null });
  }
  out.sort((a, b) => b.mtime - a.mtime);
  return out;
}
async function broadcastFiles() {
  broadcast({ type: 'files', files: await listFiles() });
}

async function handleUpload(req, res, name) {
  const declared = Number(req.headers['content-length'] || 0);
  if (declared > MAX_UPLOAD_BYTES) {
    return sendJson(res, 413, { error: `That file is over the ${MAX_UPLOAD_BYTES / 1024 / 1024} MB limit.` });
  }
  const tmp = path.join(SHARED_DIR, `.upload-${crypto.randomBytes(6).toString('hex')}`);
  let received = 0;
  req.on('data', (c) => {
    received += c.length;
    if (received > MAX_UPLOAD_BYTES) req.destroy(new Error('File is over the size limit.'));
  });
  try {
    await pipeline(req, fs.createWriteStream(tmp));
    await fsp.rename(tmp, path.join(SHARED_DIR, name));
  } catch (e) {
    await fsp.rm(tmp, { force: true });
    if (!res.headersSent) sendJson(res, 400, { error: `Upload stopped: ${e.message}` });
    return;
  }
  const by = devices.get(String(req.headers['x-harness-device'] || ''));
  fileMeta.set(name, { by: by?.name || null, color: by?.color || null });
  sendJson(res, 201, { ok: true, name });
  broadcastFiles();
}

async function handleDownload(req, res, name, url) {
  const fp = path.join(SHARED_DIR, name);
  let st;
  try { st = await fsp.stat(fp); } catch { return sendJson(res, 404, { error: `There is no shared file named ${name}.` }); }
  if (!st.isFile()) return sendJson(res, 404, { error: `There is no shared file named ${name}.` });
  const headers = { 'Accept-Ranges': 'bytes', 'Content-Type': 'application/octet-stream', 'Cache-Control': 'no-store' };
  if (url.searchParams.get('download')) headers['Content-Disposition'] = `attachment; filename*=UTF-8''${encodeURIComponent(name)}`;

  const range = req.headers.range;
  if (range) {
    const m = /^bytes=(\d*)-(\d*)$/.exec(range.trim());
    let start = m && m[1] !== '' ? Number(m[1]) : null;
    let end = m && m[2] !== '' ? Number(m[2]) : null;
    if (m && start === null && end !== null) { start = Math.max(0, st.size - end); end = st.size - 1; }
    if (m && start !== null && (end === null || end >= st.size)) end = st.size - 1;
    if (!m || start === null || start > end || start >= st.size) {
      res.writeHead(416, { 'Content-Range': `bytes */${st.size}` });
      return res.end();
    }
    res.writeHead(206, { ...headers, 'Content-Range': `bytes ${start}-${end}/${st.size}`, 'Content-Length': end - start + 1 });
    if (req.method === 'HEAD') return res.end();
    return fs.createReadStream(fp, { start, end }).pipe(res);
  }
  res.writeHead(200, { ...headers, 'Content-Length': st.size });
  if (req.method === 'HEAD') return res.end();
  fs.createReadStream(fp).pipe(res);
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.json': 'application/json', '.png': 'image/png',
  '.ico': 'image/x-icon', '.webmanifest': 'application/manifest+json',
};

async function serveStatic(req, res, pathname) {
  const rel = pathname === '/' ? '/index.html' : pathname;
  const fp = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!fp.startsWith(PUBLIC_DIR + path.sep)) return sendText(res, 403, 'Forbidden');
  let st;
  try { st = await fsp.stat(fp); } catch { return sendText(res, 404, 'Not found'); }
  if (!st.isFile()) return sendText(res, 404, 'Not found');
  res.writeHead(200, {
    'Content-Type': MIME[path.extname(fp)] || 'application/octet-stream',
    'Content-Length': st.size,
    'Cache-Control': 'no-cache',
  });
  if (req.method === 'HEAD') return res.end();
  fs.createReadStream(fp).pipe(res);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const p = url.pathname;
  try {
    if (p.startsWith('/api/')) {
      const pin = req.headers['x-harness-pin'] || url.searchParams.get('pin') || '';
      if (!pinOk(String(pin))) return sendJson(res, 401, { error: 'Wrong or missing PIN.' });
      if (p === '/api/files' && req.method === 'GET') return sendJson(res, 200, { files: await listFiles() });
      const m = /^\/api\/files\/(.+)$/.exec(p);
      if (m) {
        const name = safeName(m[1]);
        if (!name) return sendJson(res, 400, { error: 'That file name is not allowed.' });
        if (req.method === 'GET' || req.method === 'HEAD') return await handleDownload(req, res, name, url);
        if (req.method === 'PUT') return await handleUpload(req, res, name);
        if (req.method === 'DELETE') {
          try { await fsp.unlink(path.join(SHARED_DIR, name)); } catch { return sendJson(res, 404, { error: `There is no shared file named ${name}.` }); }
          fileMeta.delete(name);
          sendJson(res, 200, { ok: true });
          return broadcastFiles();
        }
      }
      return sendJson(res, 404, { error: 'Unknown endpoint.' });
    }
    if (req.method !== 'GET' && req.method !== 'HEAD') return sendText(res, 405, 'Method not allowed');
    return await serveStatic(req, res, p);
  } catch (e) {
    console.error(e);
    if (!res.headersSent) sendJson(res, 500, { error: 'Server error.' });
  }
});

// ---------------------------------------------------------------------------
// WebSocket: device registry and job traffic
// ---------------------------------------------------------------------------
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 * 1024 });

wss.on('connection', (ws) => {
  let device = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const helloTimer = setTimeout(() => { if (!device) ws.close(4001, 'No hello'); }, 10_000);

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (!device) {
      if (msg.type !== 'hello') return;
      if (!pinOk(String(msg.pin ?? ''))) {
        send(ws, { type: 'denied', reason: 'That PIN does not match the one shown where the server is running.' });
        return ws.close(4003, 'Wrong PIN');
      }
      clearTimeout(helloTimer);
      const id = /^[a-z0-9]{6,40}$/i.test(String(msg.clientId)) ? String(msg.clientId) : crypto.randomBytes(8).toString('hex');
      const previous = devices.get(id);
      if (previous) { // same browser reconnecting: retire the old socket
        requeueDevice(previous);
        devices.delete(id);
        previous.ws.terminate();
      }
      const info = msg.info && typeof msg.info === 'object' ? msg.info : {};
      device = {
        id, ws,
        name: cleanText(msg.name, 40, 'Unnamed device'),
        color: pickColor(id),
        info: {
          platform: cleanText(info.platform, 30, 'Device'),
          cores: clampInt(info.cores, 1, 512, 1),
          memoryGB: Number.isFinite(Number(info.memoryGB)) && info.memoryGB !== null ? clampNum(info.memoryGB, 0, 4096, null) : null,
          gpu: Boolean(info.gpu),
        },
        threads: clampInt(msg.threads, 1, 64, 1),
        sharing: msg.sharing !== false,
        inflight: new Set(), done: 0, busyMs: 0, joinedAt: Date.now(),
      };
      colorMemory.set(id, device.color);
      devices.set(id, device);
      const latest = runningJob() || jobs[jobs.length - 1];
      send(ws, { type: 'welcome', id, color: device.color, devices: deviceList(), jobs: jobs.map(jobSummary), snapshot: snapshot(latest) });
      send(ws, { type: 'files', files: await listFiles() });
      broadcastDevices();
      dispatch();
      return;
    }

    switch (msg.type) {
      case 'update':
        if (msg.name !== undefined) device.name = cleanText(msg.name, 40, device.name);
        if (msg.threads !== undefined) device.threads = clampInt(msg.threads, 1, 64, device.threads);
        if (msg.sharing !== undefined) device.sharing = Boolean(msg.sharing);
        broadcastDevices();
        dispatch();
        break;
      case 'result':
        acceptResult(device, msg);
        break;
      case 'failed':
        chunkFailed(device, msg);
        break;
      case 'release': { // a device stopped sharing before starting this piece
        const job = findJob(msg.jobId);
        device.inflight.delete(chunkKey(msg.jobId, msg.idx));
        if (job && job.status === 'running' && Number.isInteger(msg.idx) && job.by[msg.idx] === null
          && job.inflight.get(msg.idx)?.deviceId === device.id) {
          job.inflight.delete(msg.idx);
          if (!job.pending.includes(msg.idx)) job.pending.unshift(msg.idx);
        }
        dispatch();
        break;
      }
      case 'submit':
        try {
          await createJob(device, msg.job);
        } catch (e) {
          send(ws, { type: 'error', message: e instanceof UserError ? e.message : `Could not start the job: ${e.message}` });
        }
        break;
      case 'cancel': {
        const job = findJob(msg.jobId);
        if (job) stopJob(job, 'cancelled', `Cancelled by ${device.name}.`);
        break;
      }
      case 'getSnapshot': {
        const job = findJob(msg.jobId);
        if (job) send(ws, { type: 'snapshot', snapshot: snapshot(job) });
        break;
      }
      default:
        break;
    }
  });

  ws.on('close', () => {
    clearTimeout(helloTimer);
    if (!device) return;
    requeueDevice(device);
    if (devices.get(device.id) === device) devices.delete(device.id);
    broadcastDevices();
    dispatch();
  });
  ws.on('error', () => {});
});

// Phones pause background tabs without closing the socket. A missed ping
// drops the device so its pieces go to someone else.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 10_000).unref();

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------
function lanAddresses() {
  const out = [];
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) if (a.family === 'IPv4' && !a.internal) out.push(a.address);
  }
  return out;
}

server.listen(PORT, HOST, () => {
  const port = server.address().port;
  const urls = lanAddresses().map((ip) => `http://${ip}:${port}/?pin=${PIN}`);
  console.log('\nYourHarness is running.\n');
  console.log('Open this on every device you want in the pool:');
  for (const u of urls.length ? urls : [`http://localhost:${port}/?pin=${PIN}`]) console.log(`  ${u}`);
  console.log(`\nPIN: ${PIN}`);
  console.log(`Shared folder: ${SHARED_DIR}`);
  console.log(`Listening on port ${port}\n`);
  if (urls[0] && !process.env.NO_QR) {
    console.log('Scan with a phone camera to join:');
    qrcode.generate(urls[0], { small: true });
  }
});

function shutdown() {
  console.log('\nStopping YourHarness.');
  for (const ws of wss.clients) ws.terminate();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
