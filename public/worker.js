// One background thread. The page starts one per thread you choose to share.
import { compute } from './jobs.js';

self.onmessage = async (e) => {
  const { task, pin } = e.data;
  const t0 = performance.now();
  try {
    let value = await compute(task, { pin, origin: self.location.origin });
    if (value === undefined) value = null;
    self.postMessage({ ok: true, value, ms: Math.round(performance.now() - t0) });
  } catch (err) {
    self.postMessage({ ok: false, error: String((err && err.message) || err), ms: Math.round(performance.now() - t0) });
  }
};
