'use strict';
// The job engine's built-in operators in JavaScript, for browser tabs: batch.aggregate and
// batch.groupby over JSON Lines, CSV and text. They are a port of the Python ones
// (yourharness/engine/{records,ops,exact}.py) and must give the coordinator exactly the same
// partial state for the same bytes, because states from browsers and from computers are
// combined together and spot checks compare them byte for byte.
//
// So numbers keep Python's types: a JSON integer becomes a BigInt (Python int), anything with
// a fraction or exponent a Number (Python float), and sums are exact integers counting units
// of 2^-1074. Where JavaScript can't be sure to do exactly what Python does (a regular
// expression, digits from other scripts, a group key holding an object with numeric keys),
// it throws Unsupported and the partition goes to a computer instead. It never guesses.
//
// No DOM: this runs in a Web Worker in the page, and in Node for the tests that compare it
// with the Python operators (tests/test_browser_ops.py).
(function (root) {
  const MISSING = Symbol('missing');
  const MAX_RECORD = 64 * 1024 * 1024;
  const MAX_GROUPS = 200000;
  const CSV_FIELD_LIMIT = 131072; // Python's csv.field_size_limit()
  const INT_DIGITS_LIMIT = 4300; // Python's int max str digits

  class Unsupported extends Error {
    constructor(message) { super(message); this.name = 'Unsupported'; }
  }
  class OpError extends Error {
    constructor(message) { super(message); this.name = 'OpError'; }
  }
  class RecordError extends Error {
    constructor(offset, reason) {
      super(`the record at byte ${offset.toLocaleString('en-US')} ${reason}`);
      this.name = 'RecordError';
      this.offset = offset;
      this.reason = reason;
    }
  }

  // ---------------------------------------------------------------- JSON with Python's number types
  const WS = new Set([0x20, 0x09, 0x0a, 0x0d]);

  function parseJSON(text) {
    let i = 0;
    const n = text.length;
    const fail = (what) => { throw new SyntaxError(`${what} at character ${i}`); };
    const skip = () => { while (i < n && WS.has(text.charCodeAt(i))) i++; };
    function value() {
      skip();
      if (i >= n) fail('Expecting value');
      const c = text[i];
      if (c === '{') return object();
      if (c === '[') return array();
      if (c === '"') return string();
      if (c === 't' && text.startsWith('true', i)) { i += 4; return true; }
      if (c === 'f' && text.startsWith('false', i)) { i += 5; return false; }
      if (c === 'n' && text.startsWith('null', i)) { i += 4; return null; }
      if (c === 'N' && text.startsWith('NaN', i)) { i += 3; return NaN; }
      if (c === 'I' && text.startsWith('Infinity', i)) { i += 8; return Infinity; }
      if (c === '-' && text.startsWith('-Infinity', i)) { i += 9; return -Infinity; }
      if (c === '-' || (c >= '0' && c <= '9')) return number();
      return fail('Expecting value');
    }
    function number() {
      const m = /-?(?:0|[1-9]\d*)(\.\d+)?([eE][-+]?\d+)?/y;
      m.lastIndex = i;
      const r = m.exec(text);
      if (!r) fail('Expecting value');
      i = m.lastIndex;
      if (r[1] === undefined && r[2] === undefined) {
        if (r[0].replace('-', '').length > INT_DIGITS_LIMIT) throw new Unsupported('an integer with more digits than Python reads');
        return BigInt(r[0]);
      }
      return Number(r[0]);
    }
    function string() {
      i++;
      let out = '';
      let start = i;
      for (;;) {
        if (i >= n) fail('Unterminated string');
        const code = text.charCodeAt(i);
        if (code === 0x22) { out += text.slice(start, i); i++; return out; }
        if (code < 0x20) fail('Invalid control character');
        if (code === 0x5c) {
          out += text.slice(start, i);
          const e = text[i + 1];
          const simple = { '"': '"', '\\': '\\', '/': '/', b: '\b', f: '\f', n: '\n', r: '\r', t: '\t' }[e];
          if (simple !== undefined) { out += simple; i += 2; } else if (e === 'u') {
            const hex = text.slice(i + 2, i + 6);
            if (!/^[0-9a-fA-F]{4}$/.test(hex)) fail('Invalid \\uXXXX escape');
            out += String.fromCharCode(parseInt(hex, 16));
            i += 6;
          } else fail('Invalid \\escape');
          start = i;
          continue;
        }
        i++;
      }
    }
    function array() {
      i++;
      const out = [];
      skip();
      if (text[i] === ']') { i++; return out; }
      for (;;) {
        out.push(value());
        skip();
        if (text[i] === ',') { i++; continue; }
        if (text[i] === ']') { i++; return out; }
        fail("Expecting ',' delimiter");
      }
    }
    function object() {
      i++;
      const out = Object.create(null);
      skip();
      if (text[i] === '}') { i++; return out; }
      for (;;) {
        skip();
        if (text[i] !== '"') fail('Expecting property name enclosed in double quotes');
        const k = string();
        skip();
        if (text[i] !== ':') fail("Expecting ':' delimiter");
        i++;
        out[k] = value();
        skip();
        if (text[i] === ',') { i++; continue; }
        if (text[i] === '}') { i++; return out; }
        fail("Expecting ',' delimiter");
      }
    }
    const v = value();
    skip();
    if (i !== n) fail('Extra data');
    return v;
  }

  // A Python float inside a state. Plain JavaScript numbers in a state are counts (Python ints);
  // a float must keep its decimal point, or Python would read 5.0 back as the int 5.
  class PyFloat {
    constructor(v) { this.v = v; }
  }

  // A state as JSON text the coordinator reads back into the same Python values.
  function stateJSON(v) {
    if (v === null || v === undefined) return 'null';
    if (typeof v === 'bigint') return v.toString();
    if (v instanceof PyFloat) return floatRepr(v.v, true);
    if (typeof v === 'number') {
      if (!Number.isSafeInteger(v)) throw new Error('a count in a state is not a whole number');
      return String(v);
    }
    if (typeof v === 'boolean' || typeof v === 'string') return JSON.stringify(v);
    if (Array.isArray(v)) return `[${v.map(stateJSON).join(',')}]`;
    return `{${Object.keys(v).map((k) => `${JSON.stringify(k)}:${stateJSON(v[k])}`).join(',')}}`;
  }

  // ---------------------------------------------------------------- Python's text forms of values
  function floatRepr(x, json) {
    if (Number.isNaN(x)) return json ? 'NaN' : 'nan';
    if (x === Infinity) return json ? 'Infinity' : 'inf';
    if (x === -Infinity) return json ? '-Infinity' : '-inf';
    const neg = x < 0 || Object.is(x, -0);
    const [mant, exp] = Math.abs(x).toExponential().split('e');
    const digits = mant.replace('.', '');
    const decpt = Number(exp) + 1;
    let s;
    if (decpt <= -4 || decpt > 16) {
      const e = decpt - 1;
      s = `${digits[0]}${digits.length > 1 ? `.${digits.slice(1)}` : ''}e${e < 0 ? '-' : '+'}${String(Math.abs(e)).padStart(2, '0')}`;
    } else if (decpt <= 0) {
      s = `0.${'0'.repeat(-decpt)}${digits}`;
    } else if (decpt >= digits.length) {
      s = `${digits}${'0'.repeat(decpt - digits.length)}.0`;
    } else {
      s = `${digits.slice(0, decpt)}.${digits.slice(decpt)}`;
    }
    return neg ? `-${s}` : s;
  }

  function pyStr(v) { // str(v) for the values a filter compares as text
    if (v === null) return 'None';
    if (v === true) return 'True';
    if (v === false) return 'False';
    if (typeof v === 'bigint') return v.toString();
    if (typeof v === 'number') return floatRepr(v, false);
    if (typeof v === 'string') return v;
    throw new Unsupported("comparing a list or an object as text the way Python's str() does");
  }

  const ESC = { '"': '\\"', '\\': '\\\\', '\b': '\\b', '\f': '\\f', '\n': '\\n', '\r': '\\r', '\t': '\\t' };
  function pyJSONString(s) {
    let out = '"';
    for (const ch of s) {
      const c = ch.charCodeAt(0);
      if (ESC[ch] !== undefined) out += ESC[ch];
      else if (c < 0x20) out += `\\u${c.toString(16).padStart(4, '0')}`;
      else out += ch;
    }
    return `${out}"`;
  }
  const ARRAY_INDEX = /^(0|[1-9]\d{0,9})$/;
  function pyJSONCompact(v) { // json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    if (v === null) return 'null';
    if (v === true) return 'true';
    if (v === false) return 'false';
    if (typeof v === 'bigint') return v.toString();
    if (typeof v === 'number') return floatRepr(v, true);
    if (typeof v === 'string') return pyJSONString(v);
    if (Array.isArray(v)) return `[${v.map(pyJSONCompact).join(',')}]`;
    const keys = Object.keys(v);
    if (keys.some((k) => ARRAY_INDEX.test(k) && Number(k) < 4294967295)) {
      throw new Unsupported('a group key holding an object with numeric keys, whose order JavaScript changes');
    }
    return `{${keys.map((k) => `${pyJSONString(k)}:${pyJSONCompact(v[k])}`).join(',')}}`;
  }

  // ---------------------------------------------------------------- fields and numbers
  function getField(rec, path) {
    let cur = rec;
    for (const part of path.split('.')) {
      if (cur !== null && typeof cur === 'object' && !Array.isArray(cur) && Object.prototype.hasOwnProperty.call(cur, part)) cur = cur[part];
      else if (Array.isArray(cur) && /^[0-9]+$/.test(part) && Number(part) < cur.length) cur = cur[Number(part)];
      else if (Array.isArray(cur) && /[^0-9]/.test(part) && /^\p{Nd}+$/u.test(part)) throw new Unsupported('a field path with digits from another script');
      else return MISSING;
    }
    return cur;
  }

  // Python's str.strip(): the characters str.isspace() calls whitespace.
  const PY_SPACE = new Set([0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x85, 0xa0, 0x1680, 0x2000, 0x2001, 0x2002, 0x2003, 0x2004,
    0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000]);
  function pyStrip(s) {
    let a = 0;
    let b = s.length;
    while (a < b && PY_SPACE.has(s.charCodeAt(a))) a++;
    while (b > a && PY_SPACE.has(s.charCodeAt(b - 1))) b--;
    return s.slice(a, b);
  }
  const PY_INT = /^[+-]?\d(?:_?\d)*$/;
  const PY_FLOAT = /^[+-]?(?:(?:\d(?:_?\d)*)?\.\d(?:_?\d)*|\d(?:_?\d)*\.?)(?:[eE][+-]?\d(?:_?\d)*)?$/;
  const PY_SPECIAL = /^([+-]?)(inf|infinity|nan)$/i;

  function toNumber(v) {
    if (v === MISSING || v === null || typeof v === 'boolean') return null;
    if (typeof v === 'bigint' || typeof v === 'number') return v;
    if (typeof v !== 'string') return null;
    const s = pyStrip(v);
    if (!s) return null;
    if (/[^\x00-\x7f]/.test(s) && /\p{Nd}/u.test(s)) throw new Unsupported('a number written with digits from another script');
    if (PY_INT.test(s)) {
      const neg = s[0] === '-';
      const digits = s.replace(/^[+-]/, '').replace(/_/g, '');
      if (digits.length > INT_DIGITS_LIMIT) throw new Unsupported('an integer with more digits than Python reads');
      return neg ? -BigInt(digits) : BigInt(digits);
    }
    if (PY_FLOAT.test(s)) return Number(s.replace(/_/g, ''));
    const sp = PY_SPECIAL.exec(s);
    if (sp) {
      if (sp[2].toLowerCase() === 'nan') return NaN;
      return sp[1] === '-' ? -Infinity : Infinity;
    }
    return null;
  }

  // Python's ==: numbers by value whatever their type (True == 1 == 1.0), text with text, None with None.
  function isNum(v) { return typeof v === 'bigint' || typeof v === 'number' || typeof v === 'boolean'; }
  function asNum(v) { return typeof v === 'boolean' ? (v ? 1n : 0n) : v; }
  function pyEq(a, b) {
    if (isNum(a) && isNum(b)) {
      const x = asNum(a);
      const y = asNum(b);
      return typeof x === 'bigint' && typeof y === 'bigint' ? x === y : x == y; // eslint-disable-line eqeqeq
    }
    if (a === null || b === null) return a === b;
    if (typeof a === 'string' || typeof b === 'string') return a === b;
    if (Array.isArray(a) || Array.isArray(b)) return Array.isArray(a) && Array.isArray(b) && a.length === b.length && a.every((x, k) => pyEq(x, b[k]));
    if (typeof a === 'object' && typeof b === 'object') {
      const ka = Object.keys(a);
      return ka.length === Object.keys(b).length && ka.every((k) => Object.prototype.hasOwnProperty.call(b, k) && pyEq(a[k], b[k]));
    }
    return false;
  }
  function lt(a, b) { return a < b; } // BigInt and Number compare by exact value in JavaScript, as int and float do in Python

  // ---------------------------------------------------------------- filters
  function compilePredicate(p) {
    const { field, op } = p;
    const value = p.value;
    if (op === 'exists') return (r) => { const v = getField(r, field); return v !== MISSING && v !== null; };
    if (op === 'missing') return (r) => { const v = getField(r, field); return v === MISSING || v === null; };
    if (op === '<' || op === '<=' || op === '>' || op === '>=') {
      const target = toNumber(value);
      // JavaScript compares a BigInt with a Number by exact value, as Python compares an int with a float
      const cmp = { '<': (x) => x < target, '<=': (x) => x <= target, '>': (x) => x > target, '>=': (x) => x >= target }[op];
      return (r) => {
        const v = toNumber(getField(r, field));
        return v !== null && !(typeof v === 'number' && Number.isNaN(v)) && cmp(v);
      };
    }
    if (op === '==' || op === '!=') {
      let eq;
      if (isNum(value) && typeof value !== 'boolean') {
        eq = (r) => { const v = toNumber(getField(r, field)); return v !== null && pyEq(v, value); };
      } else {
        eq = (r) => {
          let v = getField(r, field);
          if (v === MISSING) return value === null;
          if (typeof value === 'string' && typeof v !== 'string') {
            if (v !== null && typeof v === 'object') throw new Unsupported('comparing a list or an object with text the way Python does');
            v = v === true ? 'true' : v === false ? 'false' : pyStr(v);
          }
          return pyEq(v, value);
        };
      }
      return op === '==' ? eq : (r) => !eq(r);
    }
    if (op === 'in') {
      const options = new Set(value.map(pyStr));
      return (r) => { const v = getField(r, field); return v !== MISSING && options.has(pyStr(v)); };
    }
    if (op === 'contains') {
      const needle = String(value);
      return (r) => { const v = getField(r, field); return typeof v === 'string' && v.includes(needle); };
    }
    if (op === 'regex') throw new Unsupported("regular expressions, which JavaScript and Python don't read the same way");
    throw new OpError(`unknown filter ${op}`);
  }

  function compileWhere(where) {
    const preds = (where || []).map(compilePredicate);
    return (r) => preds.every((p) => p(r));
  }

  // ---------------------------------------------------------------- exact sums
  const F64 = new Float64Array(1);
  const U32 = new Uint32Array(F64.buffer);
  const HI = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1 ? 1 : 0; // the high word on this machine

  function scaled(x) { // x * 2^1074, exactly, for a finite float
    F64[0] = x;
    const hi = U32[HI];
    const lo = U32[1 - HI];
    const e = (hi >>> 20) & 0x7ff;
    let mant = (BigInt(hi & 0xfffff) << 32n) | BigInt(lo);
    if (e) mant |= 1n << 52n;
    const units = e ? mant << BigInt(e - 1) : mant;
    return hi >>> 31 ? -units : units;
  }

  class ExactSum {
    constructor() { this.ints = 0n; this.units = 0n; this.floats = 0; this.nan = 0; this.pinf = 0; this.ninf = 0; this.n = 0; }
    add(v) {
      this.n += 1;
      if (typeof v === 'bigint') { this.ints += v; return; }
      this.floats += 1;
      if (Number.isNaN(v)) this.nan += 1;
      else if (v === Infinity) this.pinf += 1;
      else if (v === -Infinity) this.ninf += 1;
      else this.units += scaled(v);
    }
    state() { return { i: this.ints.toString(), u: this.units.toString(), f: this.floats, x: [this.nan, this.pinf, this.ninf], n: this.n }; }
  }

  function numberOut(v) {
    if (typeof v === 'number') {
      if (Number.isNaN(v)) return 'NaN';
      if (v === Infinity) return 'Infinity';
      if (v === -Infinity) return '-Infinity';
      return new PyFloat(v);
    }
    return v;
  }

  // ---------------------------------------------------------------- metrics
  function newMetric(m) {
    if (m.op === 'count') {
      let n = 0;
      return { feed(r) { if (m.field == null || ((v) => v !== MISSING && v !== null)(getField(r, m.field))) n += 1; }, state: () => ({ n }) };
    }
    if (m.op === 'sum' || m.op === 'mean') {
      const s = new ExactSum();
      let skip = 0;
      return { feed(r) { const v = toNumber(getField(r, m.field)); if (v === null) skip += 1; else s.add(v); }, state: () => ({ s: s.state(), skip }) };
    }
    if (m.op === 'min' || m.op === 'max') {
      const wantMax = m.op === 'max';
      let best = null;
      let skip = 0;
      let nan = 0;
      return {
        feed(r) {
          const v = toNumber(getField(r, m.field));
          if (v === null) skip += 1;
          else if (typeof v === 'number' && Number.isNaN(v)) nan += 1;
          else if (best === null || (wantMax ? lt(best, v) : lt(v, best))) best = v;
        },
        state: () => ({ v: numberOut(best), skip, nan }),
      };
    }
    throw new OpError(`unknown metric ${m.op}`);
  }

  function groupKey(rec, fields) {
    return pyJSONCompact(fields.map((f) => { const v = getField(rec, f); return v === MISSING ? null : v; }));
  }

  class AggregateOp {
    constructor(wl) { this.where = compileWhere(wl.where); this.metrics = wl.metrics.map(newMetric); this.seen = 0; this.matched = 0; }
    feed(r) {
      this.seen += 1;
      if (!this.where(r)) return;
      this.matched += 1;
      for (const m of this.metrics) m.feed(r);
    }
    state() { return { seen: this.seen, matched: this.matched, metrics: this.metrics.map((m) => m.state()) }; }
  }

  class GroupByOp {
    constructor(wl) { this.wl = wl; this.where = compileWhere(wl.where); this.groups = new Map(); this.seen = 0; this.matched = 0; }
    feed(r) {
      this.seen += 1;
      if (!this.where(r)) return;
      this.matched += 1;
      const key = groupKey(r, this.wl.by);
      let ms = this.groups.get(key);
      if (!ms) {
        if (this.groups.size >= MAX_GROUPS) throw new OpError(`a partition has more than ${MAX_GROUPS.toLocaleString('en-US')} distinct groups; for this many, add "shuffle": true to the group-by and use smaller partitions`);
        ms = this.wl.metrics.map(newMetric);
        this.groups.set(key, ms);
      }
      for (const m of ms) m.feed(r);
    }
    state() {
      const groups = Object.create(null);
      for (const [k, ms] of this.groups) groups[k] = ms.map((m) => m.state());
      return { seen: this.seen, matched: this.matched, groups };
    }
  }

  function makeOp(wl) {
    if (wl.type === 'batch.aggregate') return new AggregateOp(wl);
    if (wl.type === 'batch.groupby') {
      if (wl.shuffle != null) throw new Unsupported('a shuffled group-by');
      return new GroupByOp(wl);
    }
    throw new Unsupported(`${wl.type} runs on computers, not in browsers`);
  }

  // What a browser can run, decided before fetching anything.
  function check(task) {
    if ((task.runtime || {}).kind !== 'builtin') throw new Unsupported('your own code, which runs only on your computers');
    if ((task.stage || 'map') !== 'map') throw new Unsupported(`a ${task.stage} stage`);
    if (!['range', 'artifact'].includes(task.input.kind)) throw new Unsupported(`${task.input.kind} inputs`);
    if (!['jsonl', 'csv', 'text'].includes(task.format)) throw new Unsupported(`${task.format} files`);
    const op = makeOp(task.workload);
    if (task.input.halo) throw new Unsupported('a partition that reads the records before it');
    return op;
  }

  // ---------------------------------------------------------------- records from bytes
  const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
  function text(bytes, start) {
    try { return decoder.decode(bytes); } catch { throw new RecordError(start, "isn't UTF-8 text"); }
  }

  // Lines from a stream of byte chunks, pushed in as they arrive: [start, end, bytes] with the
  // dataset offsets Python's iter_lines() gives, a trailing CR removed.
  class LineSplitter {
    constructor(offset) { this.pending = new Uint8Array(0); this.pos = offset; }
    push(chunk) {
      const out = [];
      if (!chunk || !chunk.length) return out;
      let data;
      if (this.pending.length) { data = new Uint8Array(this.pending.length + chunk.length); data.set(this.pending); data.set(chunk, this.pending.length); } else data = chunk;
      let start = 0;
      for (;;) {
        const nl = data.indexOf(0x0a, start);
        if (nl < 0) break;
        let line = data.subarray(start, nl);
        if (line.length && line[line.length - 1] === 0x0d) line = line.subarray(0, line.length - 1);
        out.push([this.pos + start, this.pos + nl + 1, line]);
        start = nl + 1;
      }
      this.pending = data.slice(start);
      this.pos += start;
      if (this.pending.length > MAX_RECORD) throw new RecordError(this.pos, 'is longer than 64 MB');
      return out;
    }
    end() {
      const p = this.pending;
      this.pending = new Uint8Array(0);
      if (!p.length) return [];
      return [[this.pos, this.pos + p.length, p[p.length - 1] === 0x0d ? p.subarray(0, p.length - 1) : p]];
    }
  }

  function blank(bytes) {
    for (const b of bytes) if (!(b === 0x20 || (b >= 0x09 && b <= 0x0d))) return false;
    return true;
  }

  // Records from lines, like Python's iter_records(): line(start, end, bytes) returns the
  // record or null (a blank line, a bad record passed to onBad, a CSV record that goes on).
  class RecordReader {
    constructor(format, header, onBad) {
      this.format = format;
      this.bad = (e) => { if (!onBad) throw e; onBad(e); };
      if (format === 'csv') {
        if (!header || !header.length) throw new OpError('a CSV dataset needs its header');
        this.csv = new CsvReader(header, this.bad);
      } else if (format !== 'text' && format !== 'jsonl') throw new OpError(`unknown format ${format}`);
    }
    line(s, e, bytes) {
      if (this.csv) return this.csv.line(s, e, bytes);
      if (this.format === 'text') {
        let t;
        try { t = text(bytes, s); } catch (err) { this.bad(err); return null; }
        const rec = Object.create(null);
        rec.line = t;
        return rec;
      }
      if (blank(bytes)) return null;
      let v;
      try { v = parseJSON(text(bytes, s)); } catch (err) {
        if (err instanceof Unsupported) throw err;
        this.bad(err instanceof RecordError ? err : new RecordError(s, `isn't valid JSON (${err.message})`));
        return null;
      }
      if (v !== null && typeof v === 'object' && !Array.isArray(v)) return v;
      const rec = Object.create(null);
      rec.value = v;
      return rec;
    }
    end() { if (this.csv) this.csv.end(); }
  }

  // Python's csv reader (excel dialect, strict=True), one character at a time.
  class CsvReader {
    constructor(header, bad) {
      this.header = header;
      this.bad = bad;
      this.reset();
      this.recEnd = null;
    }
    reset() { this.fields = []; this.field = []; this.fieldLen = 0; this.state = 'start-record'; this.recStart = null; this.decodeErr = null; }
    line(s, e, bytes) {
      if (this.recStart === null) this.recStart = s;
      this.recEnd = e;
      let t;
      try { t = text(bytes, s); } catch (err) { this.decodeErr = this.decodeErr || err; t = ''; }
      if (t.includes('\0')) throw new Unsupported('a NUL character in a CSV file');
      try {
        for (const ch of `${t}\n`) this.step(ch);
        this.step(null);
      } catch (err) { // Python's reader reports the error and starts afresh with the next line
        const start = this.recStart || 0;
        this.reset();
        this.bad(new RecordError(start, `isn't valid CSV (${err.message})`));
        return null;
      }
      if (this.state !== 'start-record') return null; // a quoted field goes on to the next line
      const row = this.fields;
      const start = this.recStart;
      const decodeErr = this.decodeErr;
      this.fields = [];
      this.recStart = null;
      this.decodeErr = null;
      if (decodeErr) { this.bad(decodeErr); return null; }
      if (!row.length) return null; // a blank line
      if (row.length !== this.header.length) {
        this.bad(new RecordError(start || 0, `has ${row.length} fields, but the header has ${this.header.length}`));
        return null;
      }
      const rec = Object.create(null);
      this.header.forEach((h, k) => { rec[h] = row[k]; });
      return rec;
    }
    end() {
      if (this.state === 'in-quoted' || this.field.length) this.bad(new RecordError(this.recStart || 0, "isn't valid CSV (unexpected end of data)"));
    }
    save() { this.fields.push(this.field.join('')); this.field = []; this.fieldLen = 0; }
    add(ch) {
      this.fieldLen += 1;
      if (this.fieldLen > CSV_FIELD_LIMIT) throw new Error(`field larger than field limit (${CSV_FIELD_LIMIT})`);
      this.field.push(ch);
    }
    step(ch) { // ch: one character, or null for the end of a line
      const eol = ch === null;
      const nl = ch === '\n' || ch === '\r';
      switch (this.state) {
        case 'start-record':
          if (eol) return;
          if (nl) { this.state = 'eat-crnl'; return; }
          this.state = 'start-field';
        // falls through
        case 'start-field':
          if (eol || nl) { this.save(); this.state = eol ? 'start-record' : 'eat-crnl'; return; }
          if (ch === '"') { this.state = 'in-quoted'; return; }
          if (ch === ',') { this.save(); return; }
          this.add(ch);
          this.state = 'in-field';
          return;
        case 'in-field':
          if (eol || nl) { this.save(); this.state = eol ? 'start-record' : 'eat-crnl'; return; }
          if (ch === ',') { this.save(); this.state = 'start-field'; return; }
          this.add(ch);
          return;
        case 'in-quoted':
          if (eol) return;
          if (ch === '"') { this.state = 'quote-in-quoted'; return; }
          this.add(ch);
          return;
        case 'quote-in-quoted':
          if (ch === '"') { this.add('"'); this.state = 'in-quoted'; return; }
          if (ch === ',') { this.save(); this.state = 'start-field'; return; }
          if (eol || nl) { this.save(); this.state = eol ? 'start-record' : 'eat-crnl'; return; }
          throw new Error("',' expected after '\"'");
        case 'eat-crnl':
          if (nl) return;
          if (eol) { this.state = 'start-record'; return; }
          throw new Error("new-line character seen in unquoted field - do you need to open the file with newline=''?");
        default:
          throw new Error('bad state');
      }
    }
  }

  // One partition, its bytes pushed in as they arrive. Like engine/execute.py run_partition().
  class PartitionRun {
    constructor(op, format, header, start, bad = 'fail') {
      this.op = op;
      this.skipped = [];
      this.n = 0;
      this.lines = new LineSplitter(start);
      this.reader = new RecordReader(format, header, bad === 'skip' ? (e) => this.skipped.push({ offset: e.offset, reason: e.reason }) : null);
    }
    take(items) {
      for (const [s, e, bytes] of items) {
        const rec = this.reader.line(s, e, bytes);
        if (rec !== null) { this.op.feed(rec); this.n += 1; }
      }
    }
    push(chunk) { this.take(this.lines.push(chunk)); }
    end() {
      this.take(this.lines.end());
      this.reader.end();
      return { records: this.n, skipped: this.skipped.length, bad: this.skipped.slice(0, 20) };
    }
  }

  function runPartition(op, format, header, chunks, start, bad = 'fail') {
    const run = new PartitionRun(op, format, header, start, bad);
    for (const c of chunks) run.push(c);
    return run.end();
  }

  root.YHEngineOps = { parseJSON, stateJSON, floatRepr, pyStr, pyJSONCompact, toNumber, getField, groupKey, scaled, ExactSum, makeOp, check,
    PartitionRun, runPartition, Unsupported, OpError, RecordError, MISSING };
}(typeof self !== 'undefined' ? self : globalThis));
