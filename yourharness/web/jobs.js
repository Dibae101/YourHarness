// Compute code for every job type, run by browser workers (worker.js).
// Agents run the Python copy in yourharness/kernels.py. Both give answers that can
// be mixed in one job, so pieces of a run can come from any device.

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

// --- π: Monte Carlo, seeded per piece so a re-run piece gives the same answer.
// xoshiro128** has a 128-bit state, so pieces never run into each other's numbers
// (a 32-bit generator repeats after about 2 billion points).
export function monteCarlo(p, idx, jobId) {
  const rand = xoshiro128(hashString(`${jobId}:${idx}`));
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
    headers: { Range: `bytes=${lo}-${end - 1}` },
    credentials: 'same-origin',
    cache: 'no-store',
  });
  if (res.status !== 206 && res.status !== 200) throw new Error(`Could not read ${p.file} (HTTP ${res.status})`);
  let buf = new Uint8Array(await res.arrayBuffer());
  if (res.status === 200) buf = buf.subarray(lo, end); // server ignored the range
  // "Ignore case" folds A-Z only, the same as the agents' Python version
  const hay = bytesToBinary(p.ignoreCase ? asciiLower(buf) : buf);
  const needle = bytesToBinary(p.ignoreCase ? asciiLower(needleBytes) : needleBytes);
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
function asciiLower(bytes) {
  const out = new Uint8Array(bytes.length);
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i];
    out[i] = b >= 65 && b <= 90 ? b + 32 : b;
  }
  return out;
}

function xoshiro128(seed) {
  let x = seed | 0;
  const split = () => { // splitmix32, to spread one seed over the four state words
    x = (x + 0x9e3779b9) | 0;
    let t = x ^ (x >>> 16);
    t = Math.imul(t, 0x21f0aaad);
    t ^= t >>> 15;
    t = Math.imul(t, 0x735a2d97);
    return (t ^ (t >>> 15)) | 0;
  };
  let a = split(), b = split(), c = split(), d = split();
  return () => {
    const r = Math.imul(b, 5);
    const result = Math.imul((r << 7) | (r >>> 25), 9);
    const t = b << 9;
    c ^= a; d ^= b; b ^= c; a ^= d; c ^= t;
    d = (d << 11) | (d >>> 21);
    return (result >>> 0) / 4294967296;
  };
}

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
