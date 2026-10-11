// Compute kernels for every job type. Browser workers (worker.js) import this,
// and so does the Node smoke test, so the same code is tested in both places.

let primeCache = { upTo: 0, primes: new Uint32Array(0) };
const customCache = { code: null, fn: null };

export async function compute(task, ctx = {}) {
  switch (task.kind) {
    case 'mandelbrot': return mandelbrot(task.params, task.idx);
    case 'primes': return countPrimes(task.params, task.idx);
    case 'pi': return monteCarlo(task.params, task.idx, task.jobId);
    case 'grep': return searchFile(task.params, task.idx, ctx);
    case 'custom': return runCustom(task.params, task.idx, task.total);
    default: throw new Error(`Unknown job type: ${task.kind}`);
  }
}

// --- Fractal: one horizontal strip of rows, iteration counts as base64 Uint16
export function mandelbrot(p, idx) {
  const { width, height, maxIter, cx, cy, span, rows } = p;
  const y0 = idx * rows;
  const y1 = Math.min(height, y0 + rows);
  const out = new Uint16Array((y1 - y0) * width);
  const step = span / width;
  const left = cx - span / 2;
  const top = cy + (height * step) / 2;
  let k = 0;
  for (let y = y0; y < y1; y++) {
    const ci = top - y * step;
    for (let x = 0; x < width; x++) {
      const cr = left + x * step;
      let zr = 0, zi = 0, zr2 = 0, zi2 = 0, i = 0;
      while (i < maxIter && zr2 + zi2 <= 4) {
        zi = 2 * zr * zi + ci;
        zr = zr2 - zi2 + cr;
        zr2 = zr * zr;
        zi2 = zi * zi;
        i++;
      }
      out[k++] = i;
    }
  }
  return bytesToBase64(new Uint8Array(out.buffer));
}

// --- Primes: segmented sieve over [lo, hi)
function basePrimes(n) {
  if (primeCache.upTo >= n) return primeCache.primes;
  const sieve = new Uint8Array(n + 1);
  const list = [];
  for (let i = 2; i <= n; i++) {
    if (sieve[i]) continue;
    list.push(i);
    for (let j = i * i; j <= n; j += i) sieve[j] = 1;
  }
  primeCache = { upTo: n, primes: Uint32Array.from(list) };
  return primeCache.primes;
}

export function countPrimes(p, idx) {
  const lo = idx * p.chunkSize;
  const hi = Math.min(p.limit, lo + p.chunkSize);
  if (hi <= lo) return 0;
  const bp = basePrimes(Math.floor(Math.sqrt(p.limit)) + 1);
  const seg = new Uint8Array(hi - lo);
  for (let t = 0; t < bp.length; t++) {
    const q = bp[t];
    if (q * q >= hi) break;
    const first = Math.max(q * q, Math.ceil(lo / q) * q);
    for (let m = first; m < hi; m += q) seg[m - lo] = 1;
  }
  let count = 0;
  const start = Math.max(0, 2 - lo); // 0 and 1 are not prime
  for (let i = start; i < seg.length; i++) if (seg[i] === 0) count++;
  return count;
}

// --- π: Monte Carlo with a seeded generator, so a re-run piece gives the same answer
export function monteCarlo(p, idx, jobId) {
  let s = (hashString(String(jobId)) ^ Math.imul(idx + 1, 0x9e3779b1)) >>> 0;
  const rand = () => {
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  let hits = 0;
  for (let i = 0; i < p.samples; i++) {
    const x = rand();
    const y = rand();
    if (x * x + y * y <= 1) hits++;
  }
  return hits;
}

// --- Search: read one byte range of a shared file and count matches that start in it.
// Matches are counted per starting position (overlapping), which keeps the total
// exact no matter where the piece boundaries fall.
export async function searchFile(p, idx, ctx) {
  const lo = idx * p.chunkBytes;
  const hi = Math.min(p.size, lo + p.chunkBytes);
  const needleBytes = new TextEncoder().encode(p.term);
  const end = Math.min(p.size, hi + needleBytes.length - 1); // read a little past hi
  const res = await fetch(`${ctx.origin || ''}/api/files/${encodeURIComponent(p.file)}`, {
    headers: { Range: `bytes=${lo}-${end - 1}`, 'x-harness-pin': ctx.pin || '' },
    cache: 'no-store',
  });
  if (res.status !== 206 && res.status !== 200) throw new Error(`Could not read ${p.file} (HTTP ${res.status})`);
  let buf = new Uint8Array(await res.arrayBuffer());
  if (res.status === 200) buf = buf.subarray(lo, end); // server ignored the range
  let hay = bytesToBinary(buf);
  let needle = bytesToBinary(needleBytes);
  if (p.ignoreCase) {
    hay = hay.toLowerCase();
    needle = needle.toLowerCase();
  }
  const limit = hi - lo;
  let count = 0;
  let from = 0;
  for (;;) {
    const at = hay.indexOf(needle, from);
    if (at < 0 || at >= limit) break;
    count++;
    from = at + 1;
  }
  return count;
}

// --- Your own code: compiled once per job on each thread
export function runCustom(p, idx, total) {
  if (customCache.code !== p.code) {
    customCache.fn = new Function(
      'chunk',
      'total',
      `"use strict";\n${p.code}\n;if (typeof run !== 'function') throw new Error('Define a function named run(chunk, total).');\nreturn run(chunk, total);`,
    );
    customCache.code = p.code;
  }
  return customCache.fn(idx, total);
}

// --- helpers
function hashString(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) h = Math.imul(h ^ str.charCodeAt(i), 16777619);
  return h >>> 0;
}

function bytesToBinary(bytes) {
  let s = '';
  for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
  return s;
}

export function bytesToBase64(bytes) {
  return btoa(bytesToBinary(bytes));
}
