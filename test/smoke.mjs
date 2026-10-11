// End-to-end smoke test without a browser.
// Starts the server, connects fake devices that use the same compute code as
// the browser workers, runs every job type, and checks the answers.
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';
import { compute } from '../public/jobs.js';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const PIN = '246810';
const PORT = 18000 + Math.floor(Math.random() * 2000);
const ORIGIN = `http://127.0.0.1:${PORT}`;
const sharedDir = fs.mkdtempSync(path.join(os.tmpdir(), 'yh-shared-'));

let failures = 0;
const ok = (cond, label) => { console.log(`${cond ? 'pass' : 'FAIL'}  ${label}`); if (!cond) failures++; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const server = spawn(process.execPath, ['server.js'], {
  cwd: root,
  env: { ...process.env, PORT: String(PORT), HARNESS_PIN: PIN, SHARED_DIR: sharedDir, NO_QR: '1', HOST: '127.0.0.1' },
  stdio: ['ignore', 'pipe', 'inherit'],
});
await new Promise((resolve, reject) => {
  const t = setTimeout(() => reject(new Error('server did not start')), 8000);
  server.stdout.on('data', (d) => { if (String(d).includes('Listening on port')) { clearTimeout(t); resolve(); } });
});

class FakeDevice {
  constructor(name, threads, { stall = false } = {}) {
    this.name = name; this.threads = threads; this.stall = stall;
    this.jobs = new Map(); this.waiters = []; this.computed = 0; this.queue = []; this.running = 0;
  }
  open(pin = PIN) {
    return new Promise((resolve) => {
      this.ws = new WebSocket(`ws://127.0.0.1:${PORT}/ws`);
      this.ws.on('open', () => this.ws.send(JSON.stringify({
        type: 'hello', pin, clientId: `test${this.name}${Date.now()}`, name: this.name,
        threads: this.threads, sharing: true, info: { platform: 'Test', cores: this.threads },
      })));
      this.ws.on('message', (raw) => {
        const m = JSON.parse(raw);
        if (m.type === 'welcome' || m.type === 'denied') resolve(m);
        if (m.type === 'job') {
          this.jobs.set(m.job.id, m.job);
          this.waiters = this.waiters.filter((w) => !w(m.job));
        }
        if (m.type === 'error') this.lastError = m.message;
        if (m.type === 'chunk') { this.queue.push(m); this.pump(); }
      });
    });
  }
  pump() {
    if (this.stall) return; // takes pieces and never answers
    while (this.running < this.threads && this.queue.length) {
      const t = this.queue.shift();
      this.running++;
      setImmediate(async () => {
        const t0 = performance.now();
        try {
          const value = await compute(t, { pin: PIN, origin: ORIGIN });
          this.send({ type: 'result', jobId: t.jobId, idx: t.idx, value: value ?? null, ms: Math.round(performance.now() - t0) });
        } catch (e) {
          this.send({ type: 'failed', jobId: t.jobId, idx: t.idx, error: e.message });
        }
        this.computed++;
        this.running--;
        this.pump();
      });
    }
  }
  send(m) { if (this.ws.readyState === 1) this.ws.send(JSON.stringify(m)); }
  submit(kind, params) {
    const before = new Set(this.jobs.keys());
    this.send({ type: 'submit', job: { kind, params } });
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`${kind} job timed out`)), 60_000);
      this.waiters.push((job) => {
        if (before.has(job.id) || !['done', 'failed', 'cancelled'].includes(job.status)) return false;
        clearTimeout(t); resolve(job); return true;
      });
    });
  }
  close() { this.ws.terminate(); }
}

try {
  // Wrong PIN is refused
  const intruder = new FakeDevice('intruder', 1);
  const denied = await intruder.open('000000');
  ok(denied.type === 'denied', 'wrong PIN is refused');

  const a = new FakeDevice('A', 2);
  const b = new FakeDevice('B', 2);
  const staller = new FakeDevice('Staller', 2, { stall: true });
  ok((await a.open()).type === 'welcome', 'device A joins');
  ok((await b.open()).type === 'welcome', 'device B joins');
  ok((await staller.open()).type === 'welcome', 'a device that will drop joins');

  // Primes, with a device that grabs pieces and then disconnects
  const primesDone = a.submit('primes', { limit: 1e7, chunkSize: 1e5 });
  await sleep(150);
  staller.close();
  const primes = await primesDone;
  ok(primes.status === 'done' && primes.result.count === 664579, `primes below 10 million = ${primes.result?.count} (expect 664579), survives a dropped device`);
  ok(a.computed > 0 && b.computed > 0, `work spread across devices (A ${a.computed}, B ${b.computed})`);

  const pi = await b.submit('pi', { chunks: 40, samples: 100000 });
  ok(pi.status === 'done' && Math.abs(pi.result.estimate - Math.PI) < 0.01, `π estimate ${pi.result?.estimate}`);

  // Shared files: upload, list, range read, search
  const line = 'The quick brown fox. Error: disk full. error again. ERROR!\n';
  const body = line.repeat(60000); // ~3.4 MB, crosses several 1 MB pieces
  let res = await fetch(`${ORIGIN}/api/files/${encodeURIComponent('log sample.txt')}`, { method: 'PUT', body, headers: { 'x-harness-pin': PIN } });
  ok(res.status === 201, 'upload a shared file');
  res = await fetch(`${ORIGIN}/api/files`, { headers: { 'x-harness-pin': PIN } });
  const { files } = await res.json();
  ok(files.length === 1 && files[0].size === Buffer.byteLength(body), 'file shows up in the list');
  res = await fetch(`${ORIGIN}/api/files/${encodeURIComponent('log sample.txt')}`, { headers: { Range: 'bytes=4-8' } });
  ok(res.status === 401, 'files need the PIN');
  res = await fetch(`${ORIGIN}/api/files/${encodeURIComponent('log sample.txt')}`, { headers: { Range: 'bytes=4-8', 'x-harness-pin': PIN } });
  ok(res.status === 206 && (await res.text()) === 'quick', 'range reads work');
  res = await fetch(`${ORIGIN}/api/files/..%2F..%2Fetc%2Fpasswd`, { headers: { 'x-harness-pin': PIN } });
  ok(res.status === 404 || res.status === 400, 'path traversal is blocked');

  const grep = await a.submit('grep', { file: 'log sample.txt', term: 'error', chunkMB: 1 });
  ok(grep.status === 'done' && grep.result.count === 60000, `case-sensitive search found ${grep.result?.count} (expect 60000)`);
  const grepI = await a.submit('grep', { file: 'log sample.txt', term: 'error', ignoreCase: true, chunkMB: 1 });
  ok(grepI.status === 'done' && grepI.result.count === 180000, `case-insensitive search found ${grepI.result?.count} (expect 180000)`);

  // Custom code
  const custom = await b.submit('custom', { code: 'function run(chunk, total) { return chunk; }', chunks: 100, combine: 'sum' });
  ok(custom.status === 'done' && custom.result.value === 4950, `custom code sum = ${custom.result?.value} (expect 4950)`);
  const bad = await b.submit('custom', { code: 'function run(c) { throw new Error("boom"); }', chunks: 5 });
  ok(bad.status === 'failed' && /boom/.test(bad.error), 'custom code errors stop the job with the message');

  // Fractal
  const frac = await a.submit('mandelbrot', { width: 200, height: 120, maxIter: 500, rows: 8, cx: -0.6, cy: 0, span: 3.2 });
  ok(frac.status === 'done' && frac.result.pixels === 24000 && frac.total === 15, 'fractal renders in strips');

  // Bad input gets a clear message
  a.lastError = null;
  a.send({ type: 'submit', job: { kind: 'grep', params: { file: 'missing.txt', term: 'x' } } });
  await sleep(200);
  ok(/no shared file/i.test(a.lastError || ''), `bad job input is explained: "${a.lastError}"`);

  // Static portal
  res = await fetch(`${ORIGIN}/`);
  ok(res.status === 200 && (await res.text()).includes('YourHarness'), 'portal page is served');

  a.close();
  b.close();
} catch (e) {
  console.error(e);
  failures++;
} finally {
  server.kill('SIGTERM');
  fs.rmSync(sharedDir, { recursive: true, force: true });
}

console.log(failures ? `\n${failures} check(s) failed` : '\nAll checks passed');
process.exit(failures ? 1 : 0);
