'use strict';
// The Batch app: datasets, batch jobs over them, and what each computer can run.
// It talks to /api/engine on the coordinator; live updates arrive as "eng.*"
// messages relayed by app.js. A job is built here as a JobSpec (the same JSON the
// command line and the Python SDK send), so "Show the job" gives you a file you can reuse.
(() => {
  const $ = (sel, el = document) => el.querySelector(sel);
  const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const nf = new Intl.NumberFormat();
  const bytes = (n) => (n == null ? '' : n < 1024 ? `${n} B` : n < 2 ** 20 ? `${(n / 1024).toFixed(1)} KB` : n < 2 ** 30 ? `${(n / 2 ** 20).toFixed(1)} MB` : `${(n / 2 ** 30).toFixed(2)} GB`);
  const when = (t) => (t ? new Date(t * 1000).toLocaleString() : '');
  const dur = (a, b) => { if (!a) return ''; const s = Math.max(0, (b || Date.now() / 1000) - a); return s < 60 ? `${s.toFixed(1)} s` : `${Math.floor(s / 60)} min ${Math.round(s % 60)} s`; };
  const API_VERSION = 'yourharness.dev/v1alpha1';
  const KINDS = {
    'batch.aggregate': 'Totals: count, sum, mean, min, max',
    'batch.groupby': 'Totals per group',
    'batch.transform': 'Filter and reshape records',
    'python.map': 'Your own Python function',
    'ml.evaluate': 'Evaluate a model on labeled records',
  };
  const CODE_KINDS = ['python.map', 'ml.evaluate'];
  const STATUS = { running: 'Running', reducing: 'Combining', verifying: 'Checking', done: 'Done', failed: 'Failed', cancelled: 'Cancelled', needs_approval: 'Needs your approval' };
  const SETUP_PHASE = { queued: 'Asked', uv: 'Finding uv', python: 'Making the environment', packages: 'Installing packages', preflight: 'Checking it', ready: 'Ready', warning: 'Ready, with a warning', failed: 'Failed' };
  const DECISION = {
    RUN_LOCAL: 'Kept on one computer', RUN_DISTRIBUTED: 'Spread over computers', WAIT_FOR_ELIGIBLE_WORKER: 'Waiting for a computer that can run it',
    NEEDS_SETUP_APPROVAL: 'Needs your approval to set up', UNSUPPORTED: 'Not supported', POLICY_DENIED: 'Not allowed',
  };
  const B = { jobs: new Map(), datasets: [], models: [], devices: [], tab: 'jobs', detail: null, loaded: false };
  const pct = (x) => (x == null ? '' : `${(x * 100).toFixed(1)}%`);

  async function api(method, path, body, raw) {
    const opts = { method, credentials: 'same-origin', headers: {} };
    if (raw !== undefined) { opts.body = raw; opts.headers['Content-Type'] = 'application/octet-stream'; } else if (body !== undefined) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
    const res = await fetch(path, opts);
    const type = res.headers.get('Content-Type') || '';
    const data = type.includes('application/json') ? await res.json() : await res.text();
    if (!res.ok) throw new Error((data && data.error) || `The coordinator answered ${res.status}.`);
    return data;
  }

  async function load() {
    try {
      const data = await api('GET', '/api/engine');
      B.jobs = new Map(data.jobs.map((j) => [j.id, j]));
      B.datasets = data.datasets;
      B.models = data.models || [];
      B.devices = data.devices || [];
      B.storage = data.storage;
      B.loaded = true;
      renderAll();
    } catch (e) {
      $('#bj-jobs').innerHTML = `<p class="error">${esc(e.message)}</p>`;
    }
  }

  // ---------------------------------------------------------------- the frame
  function build() {
    $('#bj-body').innerHTML = `
      <nav class="ag-tabs" role="tablist" aria-label="Batch">
        <button type="button" role="tab" data-tab="jobs">Jobs</button>
        <button type="button" role="tab" data-tab="new">New job</button>
        <button type="button" role="tab" data-tab="datasets">Datasets</button>
        <button type="button" role="tab" data-tab="computers">Computers</button>
      </nav>
      <section class="ag-pane" data-pane="jobs"><div id="bj-jobs" class="stack"></div>
        <section id="bj-detail" class="panel" hidden></section></section>
      <section class="ag-pane" data-pane="new" hidden>
        <form id="bj-form" class="stack" novalidate>
          <section class="panel"><h2>What to work on</h2>
            <div class="ag-grid">
              <label class="field"><span>Job name</span><input id="bj-name" maxlength="120" placeholder="For example: Sales by region"></label>
              <label class="field"><span>What to compute</span><select id="bj-kind">${Object.entries(KINDS).map(([k, v]) => `<option value="${k}">${esc(v)}</option>`).join('')}</select></label>
              <label class="field" id="bj-input-field"><span>Input</span><select id="bj-input"></select></label>
            </div>
            <label class="field" id="bj-items-field" hidden><span>Items, as JSON: a list, or an object of parameter lists to try every combination of</span><textarea id="bj-items" rows="3" spellcheck="false">[1, 2, 3, 4, 5, 6, 7, 8]</textarea></label>
          </section>
          <section class="panel" data-for="batch.aggregate batch.groupby"><h2>Totals</h2>
            <label class="field" data-for="batch.groupby"><span>Group by (field names, separated by commas)</span><input id="bj-by" placeholder="region"></label>
            <div id="bj-metrics" class="stack"></div>
            <button class="btn ghost small" type="button" data-act="add-metric">Add a total</button>
          </section>
          <section class="panel" data-for="batch.transform"><h2>Reshape records</h2>
            <div class="ag-grid">
              <label class="field"><span>Keep only these fields (commas; empty keeps all)</span><input id="bj-select" placeholder="id, amount"></label>
              <label class="field"><span>Rename (old=new, one per line)</span><textarea id="bj-rename" rows="2" spellcheck="false"></textarea></label>
              <label class="field"><span>Rolling mean of field</span><input id="bj-roll-field" placeholder="price"></label>
              <label class="field"><span>…over the last N records</span><input id="bj-roll-window" type="number" min="1" max="10000" value="10"></label>
            </div>
          </section>
          <section class="panel" data-for="python.map"><h2>Your function</h2>
            <label class="field"><span>Python code: define the function, and import what it needs inside it</span><textarea id="bj-code" rows="8" spellcheck="false">def process(record):
    return record
</textarea></label>
            <div class="ag-grid">
              <label class="field"><span>Function name</span><input id="bj-fn" value="process"></label>
              <label class="field"><span>Combine the results by</span><select id="bj-reducer">
                <option value="collect">Collecting them in input order</option><option value="concat">Writing the records it returns</option>
                <option value="sum">Adding up the numbers it returns</option><option value="count">Counting True results</option>
                <option value="merge_counts">Adding up name → count dicts</option><option value="best">Keeping the best result</option></select></label>
              <label class="field" data-reducer="best"><span>Best by key</span><input id="bj-best-key" placeholder="score"></label>
              <label class="field" data-reducer="best"><span>Best is</span><select id="bj-best-order"><option value="max">the highest</option><option value="min">the lowest</option></select></label>
              <label class="field"><span>Records per call</span><input id="bj-batch" type="number" min="1" value="1"></label>
            </div>
          </section>
          <section class="panel" data-for="ml.evaluate"><h2>The model and the labels</h2>
            <div class="ag-grid">
              <label class="field"><span>Model (add one in Datasets)</span><select id="bj-model"></select></label>
              <label class="field"><span>Field with the true label</span><input id="bj-label" value="label"></label>
              <label class="field"><span>Features the model reads, in order (commas; empty: the whole record)</span><input id="bj-features" placeholder="x1, x2"></label>
              <label class="field"><span>Sample id field (empty: the record's position)</span><input id="bj-id" placeholder="id"></label>
              <label class="field"><span>Records per predict call</span><input id="bj-ml-batch" type="number" min="1" value="64"></label>
              <label class="field"><span>Label names, in the model's output order (TorchScript; optional)</span><input id="bj-labels" placeholder="cat, dog"></label>
            </div>
            <label class="bj-check"><input type="checkbox" id="bj-preds"> Also write every prediction (id, label, prediction, correct) for download</label>
          </section>
          <section class="panel" data-for="python.map ml.evaluate"><h2>Python</h2>
            <div class="ag-grid">
              <label class="field"><span>Python to run it with</span><select id="bj-runtime">
                <option value="native-python">The Python YourHarness runs on, with its packages</option>
                <option value="managed-python">An environment of its own: a Python version and packages</option></select></label>
              <label class="field" data-runtime="managed-python"><span>Python version</span><input id="bj-pyver" value="3.12" maxlength="5" placeholder="3.12"></label>
            </div>
            <label class="field" data-runtime="managed-python"><span>Packages, one per line (pin versions so every computer gets the same)</span><textarea id="bj-reqs" rows="3" spellcheck="false" placeholder="scikit-learn==1.5.2"></textarea></label>
            <p class="hint" id="bj-rt-hint">Your code runs only on your own computers, with the Python YourHarness runs on and its packages (NumPy is there).</p>
          </section>
          <section class="panel" data-for="batch.aggregate batch.groupby batch.transform"><h2>Only records where</h2>
            <div id="bj-where" class="stack"></div>
            <button class="btn ghost small" type="button" data-act="add-where">Add a condition</button>
          </section>
          <section class="panel"><h2>Where it runs</h2>
            <div class="ag-grid">
              <label class="field"><span>Computers</span><select id="bj-mode"><option value="automatic">Decide for me: one computer or several, whichever is faster</option>
                <option value="distributed">Spread it over every computer that can run it</option><option value="local">Keep it on one computer</option></select></label>
              <label class="field"><span>Whose computers</span><select id="bj-trust"><option value="owned-only">Only mine</option>
                <option value="owned-and-friends">Mine and friends' (only for datasets marked shareable)</option></select></label>
              <label class="field"><span>Records that can't be read</span><select id="bj-bad"><option value="fail">Stop the job and say where</option><option value="skip">Skip them and count them</option></select></label>
              <label class="field"><span>Recompute some partitions here to check them</span><input id="bj-spot" type="number" min="0" max="1000" value="2"></label>
            </div>
            <details class="ag-more"><summary>Partitions and retries</summary><div class="ag-grid">
              <label class="field"><span>Partition size (MB; empty: sized automatically)</span><input id="bj-target" type="number" min="0.001" step="any"></label>
              <label class="field"><span>Items per partition (empty: automatic)</span><input id="bj-per" type="number" min="1"></label>
              <label class="field"><span>Tries per partition</span><input id="bj-tries" type="number" min="1" max="20" value="3"></label>
              <label class="field"><span>Time limit per partition (seconds)</span><input id="bj-timeout" type="number" min="5" value="900"></label>
            </div></details>
          </section>
          <section id="bj-plan" class="panel" hidden></section>
          <div class="ag-actions">
            <button class="btn ghost" type="button" data-act="show-spec">Show the job</button>
            <button class="btn ghost" type="button" data-act="plan">Check where it would run</button>
            <button class="btn primary" type="submit">Start the job</button>
          </div>
          <p id="bj-status" class="hint" aria-live="polite"></p>
          <pre id="bj-spec" class="ag-log" hidden tabindex="0"></pre>
        </form>
      </section>
      <section class="ag-pane" data-pane="datasets" hidden>
        <section class="panel"><h2>Add a dataset</h2>
          <p class="hint">Text (a record per line), JSON Lines (an object per line) or CSV with a header. The file is stored on the coordinator's computer and indexed once, so jobs can cut it into partitions without reading it all.</p>
          <div class="ag-grid">
            <label class="field"><span>File from this device</span><input id="bj-file" type="file"></label>
            <label class="field"><span>Or a file already in the pool</span><select id="bj-import"></select></label>
            <label class="field"><span>Format</span><select id="bj-format"><option value="">From the file name</option><option value="jsonl">JSON Lines</option><option value="csv">CSV</option><option value="text">Text lines</option></select></label>
            <label class="field"><span>Who may process it</span><select id="bj-class"><option value="owned-private">Only my computers</option><option value="shareable">Also friends' computers, for jobs that allow them</option></select></label>
          </div>
          <div class="ag-actions"><button class="btn primary" type="button" data-act="add-dataset">Add the dataset</button></div>
          <p id="bj-ds-status" class="hint" aria-live="polite"></p>
        </section>
        <div id="bj-datasets" class="stack"></div>
        <section class="panel"><h2>Add a model</h2>
          <p class="hint">For “Evaluate a model”. A Python model is a model.py that defines <code>predict(model, batch)</code> (one prediction per sample) and, if it needs one, <code>load(folder)</code>; or a .zip with model.py at its top and the files it reads. A TorchScript model is a .pt saved with torch.jit.save. Every computer checks the model's SHA-256 before using it.</p>
          <div class="ag-grid">
            <label class="field"><span>Model file</span><input id="bj-model-file" type="file" accept=".py,.zip,.pt,.pth"></label>
            <label class="field"><span>Kind</span><select id="bj-model-adapter"><option value="">From the file: .py and .zip are Python, .pt is TorchScript</option><option value="python">Python (model.py)</option><option value="torchscript">TorchScript</option></select></label>
          </div>
          <div class="ag-actions"><button class="btn primary" type="button" data-act="add-model">Add the model</button></div>
          <p id="bj-model-status" class="hint" aria-live="polite"></p>
        </section>
        <div id="bj-models" class="stack"></div>
      </section>
      <section class="ag-pane" data-pane="computers" hidden>
        <section class="panel"><h2>What each computer can run</h2>
          <div class="scroll-x"><table class="res-table ag-table"><thead><tr><th>Computer</th><th>Whose</th><th>Threads</th><th>Can run</th><th>Busy</th></tr></thead><tbody id="bj-comp"></tbody></table></div>
          <p class="hint" id="bj-storage"></p>
        </section>
      </section>`;
    addMetric({ op: 'count' });
    const body = $('#bj-body');
    body.addEventListener('click', onClick);
    body.addEventListener('change', onChange);
    $('#bj-form').addEventListener('submit', (e) => { e.preventDefault(); start(); });
    showKind();
  }

  function showTab(tab) {
    B.tab = tab;
    document.querySelectorAll('#bj-body [data-tab]').forEach((b) => b.setAttribute('aria-selected', String(b.dataset.tab === tab)));
    document.querySelectorAll('#bj-body [data-pane]').forEach((p) => { p.hidden = p.dataset.pane !== tab; });
    if (tab === 'datasets') loadImportable();
  }

  // ---------------------------------------------------------------- jobs
  function badge(status) {
    const cls = { done: 'ok', failed: 'bad', running: 'live', reducing: 'live', verifying: 'live', needs_approval: 'warn' }[status] || '';
    return `<span class="ag-badge ${cls}">${esc(STATUS[status] || status)}</span>`;
  }

  function resultText(j) {
    if (j.status !== 'done') return '';
    const r = j.result || {};
    if (j.workload === 'ml.evaluate' && r.classes) {
      return `<b>Accuracy</b> ${esc(pct(r.accuracy))} (${nf.format(r.correct)} of ${nf.format(r.samples)}) · <b>macro F1</b> ${esc((r.macro.f1).toFixed(3))} · ${nf.format(r.classes.length)} labels`
        + `${r.unlabeled ? ` · ${nf.format(r.unlabeled)} without a label left out` : ''}${j.outputs ? ` · ${nf.format(j.outputs.records)} predictions written` : ''}`;
    }
    if (r.values) return Object.entries(r.values).map(([k, v]) => `<b>${esc(k)}</b> ${esc(typeof v === 'number' ? nf.format(v) : JSON.stringify(v))}`).join(' · ');
    if (r.groups) return `${nf.format(r.groupsTotal || r.groups.length)} groups`;
    if (j.outputs) return `${nf.format(j.outputs.records)} records written (${bytes(j.outputs.bytes)})`;
    if ('value' in r) { const s = JSON.stringify(r.value); return esc(s.length > 200 ? `${s.slice(0, 200)}…` : s); }
    return '';
  }

  // A job with its own Python environment waits until you approve preparing it; this
  // shows exactly what each computer would run before you do.
  function setupCard(j) {
    if (j.status === 'needs_approval' && (j.setupPlan || []).length) {
      const rt = j.runtime || {};
      return `<div class="ag-warn bj-setup" data-setup="${esc(j.id)}">
        <p><b>Approve the setup to start.</b> It runs in Python ${esc(rt.python || '')}${(rt.requirements || []).length ? ` with ${esc(rt.requirements.join(', '))}` : ' with no extra packages'}, which no computer has prepared yet. Approving runs, on each computer you tick:</p>
        ${j.setupPlan.map((s) => `<label class="bj-check"><input type="checkbox" data-setup-device="${esc(s.id)}" checked> <b>${esc(s.name)}</b></label>
          <ol class="bj-steps">${s.steps.map((x) => `<li><code>${esc(x)}</code></li>`).join('')}</ol>`).join('')}
        <label class="bj-check"><input type="checkbox" data-setup-downloads> Let them download what's missing. ${esc(j.setupPlan[0].downloads)}</label>
        <p class="hint">Without downloads, only a uv and a Python ${esc(rt.python || '')} already on the computer are used, and packages come from uv's cache. Nothing is installed into the computer's own Python.</p>
        <div class="ag-actions"><button class="btn primary small" type="button" data-act="approve-setup" data-id="${esc(j.id)}">Approve the setup</button></div></div>`;
    }
    if (!(j.setup || []).length || ['done', 'cancelled'].includes(j.status)) return '';
    return `<p class="hint">Python environment: ${j.setup.map((s) => `${esc(s.device)} ${esc(SETUP_PHASE[s.phase] || s.phase)}${s.phase === 'ready' || !s.note ? '' : ` (${esc(s.note)})`}${s.error ? `: <span class="error">${esc(s.error)}</span>` : ''}`).join(' · ')}</p>`;
  }

  function jobCard(j) {
    const done = (j.counts && j.counts.done) || 0;
    const pct = j.total ? Math.round((done / j.total) * 100) : 100;
    const acts = [`<button class="btn ghost small" type="button" data-act="detail" data-id="${esc(j.id)}">Details</button>`];
    if (['running', 'reducing', 'verifying', 'needs_approval'].includes(j.status)) acts.push(`<button class="btn ghost small danger" type="button" data-act="cancel" data-id="${esc(j.id)}">Cancel</button>`);
    if (['failed', 'cancelled'].includes(j.status)) acts.push(`<button class="btn ghost small" type="button" data-act="retry" data-id="${esc(j.id)}">Retry</button>`);
    if (j.status === 'done') acts.push(`<a class="btn ghost small" href="/api/engine/jobs/${encodeURIComponent(j.id)}/result" download>Download the result</a>`);
    if (['done', 'failed', 'cancelled'].includes(j.status)) acts.push(`<button class="btn ghost small" type="button" data-act="delete" data-id="${esc(j.id)}">Delete</button>`);
    const who = (j.perDevice || []).map((d) => `${esc(d.name)} ${nf.format(d.n)}`).join(' · ');
    const input = j.dataset ? `${esc(j.dataset.name)} (${bytes(j.dataset.size)})` : j.items != null ? `${nf.format(j.items)} items` : '';
    const validation = j.validation ? `<p class="hint">${j.validation.level === 'spot-checked' ? 'Spot-checked' : 'Checked'}: ${esc((j.validation.checks || []).slice(-1)[0] || '')}</p>` : '';
    return `<section class="panel ag-run" data-job="${esc(j.id)}">
      <div class="panel-head"><h2>${esc(j.name)} ${badge(j.status)}</h2><div class="ag-acts">${acts.join(' ')}</div></div>
      <p class="hint">${esc(KINDS[j.workload] || j.workload)} · ${input} · ${esc(DECISION[j.decision] || j.decision || '')} · ${esc(when(j.createdAt))} by ${esc(j.createdBy || '?')}${j.finishedAt ? ` · took ${dur(j.startedAt, j.finishedAt)}` : ''}</p>
      <div class="bj-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pct}"><span style="width:${pct}%"></span></div>
      <p class="hint">${nf.format(done)} of ${nf.format(j.total)} partitions${who ? ` · ${who}` : ''}</p>
      ${j.note ? `<p>${esc(j.note)}</p>` : ''}${j.error ? `<p class="error">${esc(j.error)}</p>` : ''}
      ${setupCard(j)}${j.verify ? `<p class="hint">Checking: ${nf.format(j.verify.matched)} of ${nf.format(j.verify.sample)} partitions run again and matched${j.verify.running.length ? ` · running on ${esc(j.verify.running.join(', '))}` : ''}. The result is shown once every check matched.</p>` : ''}
      ${resultText(j) ? `<p>${resultText(j)}</p>` : ''}${validation}</section>`;
  }

  function renderJobs() {
    const box = $('#bj-jobs');
    if (!box) return;
    const jobs = [...B.jobs.values()].sort((a, b) => b.createdAt - a.createdAt);
    box.innerHTML = jobs.length ? jobs.map(jobCard).join('')
      : '<section class="panel"><h2>No batch jobs yet</h2><p class="hint">Add a dataset, then start a job in “New job”. Try “Totals” first: it counts and adds up fields across every record, spread over your computers.</p></section>';
  }

  function renderJob(id) {
    const j = B.jobs.get(id);
    const old = document.querySelector(`#bj-jobs [data-job="${CSS.escape(id)}"]`);
    if (!j || !old) { renderJobs(); return; }
    const ticks = new Map([...old.querySelectorAll('.bj-setup input[type="checkbox"]')].map((c) => [c.dataset.setupDevice || 'downloads', c.checked]));
    const tmp = document.createElement('div');
    tmp.innerHTML = jobCard(j);
    const card = tmp.firstElementChild;
    card.querySelectorAll('.bj-setup input[type="checkbox"]').forEach((c) => { const k = c.dataset.setupDevice || 'downloads'; if (ticks.has(k)) c.checked = ticks.get(k); });
    old.replaceWith(card);
  }

  async function approveSetup(id) {
    const box = document.querySelector(`[data-setup="${CSS.escape(id)}"]`);
    if (!box) return;
    const devices = [...box.querySelectorAll('[data-setup-device]')].filter((c) => c.checked).map((c) => c.dataset.setupDevice);
    if (!devices.length) { alert('Tick at least one computer.'); return; }
    const downloads = !!(box.querySelector('[data-setup-downloads]') || {}).checked;
    const { job } = await api('POST', `/api/engine/jobs/${encodeURIComponent(id)}/approve-setup`, { devices, downloads });
    B.jobs.set(job.id, job);
    renderJob(job.id);
  }

  async function showDetail(id) {
    const box = $('#bj-detail');
    box.hidden = false;
    box.innerHTML = '<p class="hint">Loading…</p>';
    B.detail = id;
    try {
      const { job } = await api('GET', `/api/engine/jobs/${encodeURIComponent(id)}`);
      if (B.detail !== id) return;
      const r = job.result || {};
      const groups = r.groups ? `<div class="scroll-x"><table class="res-table ag-table"><thead><tr><th>Group</th>${Object.keys(r.groups[0] || {}).filter((k) => k !== 'key').map((k) => `<th>${esc(k)}</th>`).join('')}</tr></thead><tbody>${r.groups.slice(0, 100).map((g) => `<tr><td>${esc(JSON.stringify(g.key))}</td>${Object.entries(g).filter(([k]) => k !== 'key').map(([, v]) => `<td>${esc(typeof v === 'number' ? nf.format(v) : JSON.stringify(v))}</td>`).join('')}</tr>`).join('')}</tbody></table></div>${job.resultTruncated ? '<p class="hint">The first 100 groups; download the result for all of them.</p>' : ''}` : '';
      const plan = job.plan || {};
      box.innerHTML = `<div class="panel-head"><h2>${esc(job.name)}</h2><button class="btn ghost small" type="button" data-act="close-detail">Close</button></div>
        ${(plan.reasons || []).length ? `<p class="hint">${esc(plan.reasons.join(' '))}</p>` : ''}
        ${groups}${job.workload === 'ml.evaluate' && r.classes ? scores(r) : ''}${provenance(job.provenance)}
        <div class="scroll-x"><table class="res-table ag-table"><thead><tr><th>#</th><th>Status</th><th>Computer</th><th>Size</th><th>Records</th><th>Time</th><th>Tries</th></tr></thead><tbody>
        ${job.partitions.map((p) => `<tr><td>${p.idx + 1}</td><td>${badge(p.status === 'leased' ? 'running' : p.status)}${p.resumed ? ' <span class="hint">resumed</span>' : ''}${p.error ? `<div class="ag-detail error">${esc(p.error)}</div>` : ''}</td>
          <td>${esc(p.device || '')}</td><td>${bytes(p.bytes)}</td><td>${p.records == null ? '' : nf.format(p.records)}</td><td>${p.seconds == null ? '' : `${p.seconds} s`}</td>
          <td>${p.attempt}${p.moves ? ` (${p.moves} moved)` : ''}</td></tr>`).join('')}</tbody></table></div>
        <details class="ag-more"><summary>Events</summary><pre class="ag-log">${esc(job.events.map((e) => `${new Date(e.ts * 1000).toLocaleTimeString()}  ${e.type}${e.partition ? ` ${e.partition}` : ''}  ${JSON.stringify(e.data)}`).join('\n'))}</pre></details>
        <details class="ag-more"><summary>The job as JSON</summary><pre class="ag-log">${esc(JSON.stringify(job.spec, null, 2))}</pre></details>`;
      box.scrollIntoView({ block: 'nearest' });
    } catch (e) { box.innerHTML = `<p class="error">${esc(e.message)}</p>`; }
  }

  // A model's scores: per label, then the confusion matrix (rows: true labels, columns: predictions).
  function scores(r) {
    const f = (x) => esc(x.toFixed(3));
    const rows = r.classes.slice(0, 200).map((c) => `<tr><td>${esc(c.label)}</td><td>${f(c.precision)}</td><td>${f(c.recall)}</td><td>${f(c.f1)}</td><td>${nf.format(c.support)}</td></tr>`).join('');
    let matrix = '';
    if (r.confusion && r.confusion.labels.length <= 30) {
      const L = r.confusion.labels;
      matrix = `<details class="ag-more" open><summary>Confusion matrix (rows: true label, columns: predicted)</summary><div class="scroll-x"><table class="res-table ag-table">
        <thead><tr><th></th>${L.map((l) => `<th>${esc(l)}</th>`).join('')}</tr></thead><tbody>
        ${r.confusion.rows.map((row, i) => `<tr><th>${esc(L[i])}</th>${row.map((n, k) => `<td${i === k ? ' class="bj-diag"' : ''}>${nf.format(n)}</td>`).join('')}</tr>`).join('')}</tbody></table></div></details>`;
    }
    return `<p><b>Accuracy</b> ${esc(pct(r.accuracy))} · <b>macro</b> precision ${f(r.macro.precision)} recall ${f(r.macro.recall)} F1 ${f(r.macro.f1)} · <b>weighted</b> F1 ${f(r.weighted.f1)}</p>
      <div class="scroll-x"><table class="res-table ag-table"><thead><tr><th>Label</th><th>Precision</th><th>Recall</th><th>F1</th><th>Support</th></tr></thead><tbody>${rows}</tbody></table></div>
      ${matrix}${r.latency && r.latency.msPerRecord != null ? `<p class="hint">Predicting took ${esc(String(r.latency.msPerRecord))} ms per record on average (${esc(r.latency.note)})</p>` : ''}`;
  }

  function provenance(p) {
    if (!p) return '';
    const bits = [];
    if (p.dataset) bits.push(`data ${esc(p.dataset.name || p.dataset.ref)} <code>${esc((p.dataset.sha256 || '').slice(0, 12))}</code>`);
    if (p.model) bits.push(`model ${esc(p.model.name || p.model.ref)} <code>${esc((p.model.sha256 || '').slice(0, 12))}</code>`);
    if (p.code) bits.push(`code <code>${esc(p.code.sha256.slice(0, 12))}</code>`);
    if (p.environment && p.environment.id) bits.push(`Python ${esc(p.environment.python)} environment <code>${esc(p.environment.id)}</code>`);
    return bits.length ? `<p class="hint">Made from: ${bits.join(' · ')} (SHA-256 prefixes).</p>` : '';
  }

  // ---------------------------------------------------------------- building a job
  const METRIC_OPS = { count: 'Count', sum: 'Sum', mean: 'Mean', min: 'Smallest', max: 'Largest' };
  const WHERE_OPS = { '==': 'is', '!=': 'is not', '>': '>', '>=': '≥', '<': '<', '<=': '≤', contains: 'contains', regex: 'matches the regex', exists: 'is present', missing: 'is missing' };

  function addMetric(m = {}) {
    const row = document.createElement('div');
    row.className = 'copyrow bj-row';
    row.innerHTML = `<select data-m="op">${Object.entries(METRIC_OPS).map(([k, v]) => `<option value="${k}">${v}</option>`).join('')}</select>
      <input data-m="field" placeholder="field (empty for Count: every record)"><input data-m="as" placeholder="name in the result (optional)">
      <button class="btn ghost small" type="button" data-act="remove-row" aria-label="Remove">×</button>`;
    $('[data-m="op"]', row).value = m.op || 'sum';
    if (m.field) $('[data-m="field"]', row).value = m.field;
    $('#bj-metrics').appendChild(row);
  }

  function addWhere() {
    const row = document.createElement('div');
    row.className = 'copyrow bj-row';
    row.innerHTML = `<input data-w="field" placeholder="field"><select data-w="op">${Object.entries(WHERE_OPS).map(([k, v]) => `<option value="${esc(k)}">${esc(v)}</option>`).join('')}</select>
      <input data-w="value" placeholder="value"><button class="btn ghost small" type="button" data-act="remove-row" aria-label="Remove">×</button>`;
    $('#bj-where').appendChild(row);
  }

  const list = (s) => String(s || '').split(',').map((x) => x.trim()).filter(Boolean);

  function value(text) {
    const t = String(text).trim();
    if (t !== '' && !Number.isNaN(Number(t))) return Number(t);
    if (t === 'true' || t === 'false') return t === 'true';
    return t;
  }

  function buildJob() {
    const kind = $('#bj-kind').value;
    const wl = { type: kind };
    if (kind === 'batch.aggregate' || kind === 'batch.groupby') {
      wl.metrics = [...document.querySelectorAll('#bj-metrics .bj-row')].map((r) => {
        const m = { op: $('[data-m="op"]', r).value };
        const f = $('[data-m="field"]', r).value.trim();
        const as = $('[data-m="as"]', r).value.trim();
        if (f) m.field = f;
        if (as) m.as = as;
        return m;
      });
      if (kind === 'batch.groupby') wl.by = list($('#bj-by').value);
    } else if (kind === 'batch.transform') {
      if (list($('#bj-select').value).length) wl.select = list($('#bj-select').value);
      const ren = {};
      $('#bj-rename').value.split('\n').forEach((line) => { const [a, b] = line.split('=').map((x) => (x || '').trim()); if (a && b) ren[a] = b; });
      if (Object.keys(ren).length) wl.rename = ren;
      if ($('#bj-roll-field').value.trim()) wl.rolling = { field: $('#bj-roll-field').value.trim(), window: Number($('#bj-roll-window').value) || 10 };
    } else if (kind === 'ml.evaluate') {
      wl.label = $('#bj-label').value.trim() || 'label';
      if (list($('#bj-features').value).length) wl.features = list($('#bj-features').value);
      if ($('#bj-id').value.trim()) wl.id = $('#bj-id').value.trim();
      wl.batch = Number($('#bj-ml-batch').value) || 64;
      wl.predictions = $('#bj-preds').checked;
      if (list($('#bj-labels').value).length) wl.labels = list($('#bj-labels').value).map(value);
    } else {
      wl.code = $('#bj-code').value;
      wl.function = $('#bj-fn').value.trim() || 'process';
      wl.reducer = $('#bj-reducer').value;
      wl.batch = Number($('#bj-batch').value) || 1;
      if (wl.reducer === 'best') wl.bestBy = { key: $('#bj-best-key').value.trim(), order: $('#bj-best-order').value };
    }
    let runtime;
    if (CODE_KINDS.includes(kind) && $('#bj-runtime').value === 'managed-python') {
      const python = $('#bj-pyver').value.trim() || '3.12';
      if (!/^3\.\d{1,2}$/.test(python)) throw new Error('The Python version should look like 3.12.');
      const requirements = $('#bj-reqs').value.split('\n').map((x) => x.trim()).filter((x) => x && !x.startsWith('#'));
      const bad = requirements.find((r) => !/^[A-Za-z0-9]/.test(r));
      if (bad) throw new Error(`“${bad}” isn't a package requirement. Write one package per line, like numpy==2.1.3.`);
      runtime = { kind: 'managed-python', python, requirements };
    }
    const where = [...document.querySelectorAll('#bj-where .bj-row')].map((r) => {
      const w = { field: $('[data-w="field"]', r).value.trim(), op: $('[data-w="op"]', r).value };
      if (!['exists', 'missing'].includes(w.op)) w.value = ['contains', 'regex'].includes(w.op) ? $('[data-w="value"]', r).value : value($('[data-w="value"]', r).value);
      return w;
    }).filter((w) => w.field);
    if (where.length && !CODE_KINDS.includes(kind)) wl.where = where;
    if (kind !== 'python.map' && $('#bj-input').value === '__items__') throw new Error('Items work with your own Python function. Pick a dataset for this kind of job.');
    const input = $('#bj-input').value;
    let inputs;
    if (input === '__items__') {
      let items;
      try { items = JSON.parse($('#bj-items').value); } catch (e) { throw new Error(`The items aren't valid JSON: ${e.message}`); }
      inputs = Array.isArray(items) ? { items } : { grid: items };
    } else {
      if (!input) throw new Error('Add a dataset first (Datasets tab), or pick items for your own function.');
      inputs = { dataset: { ref: input } };
    }
    if (kind === 'ml.evaluate') {
      if (!$('#bj-model').value) throw new Error('Add a model first (Datasets tab, “Add a model”).');
      inputs.model = { ref: $('#bj-model').value };
    }
    const partitioning = {};
    if (Number($('#bj-target').value) > 0) partitioning.targetBytes = Math.max(1024, Math.round(Number($('#bj-target').value) * 2 ** 20));
    if (Number($('#bj-per').value) > 0) partitioning.itemsPerPartition = Number($('#bj-per').value);
    return {
      apiVersion: API_VERSION, kind: 'Job', metadata: { name: $('#bj-name').value.trim() || KINDS[kind] },
      spec: {
        mode: $('#bj-mode').value, workload: wl, inputs, partitioning, ...(runtime ? { runtime } : {}),
        security: { allowedTrust: CODE_KINDS.includes(kind) ? 'owned-only' : $('#bj-trust').value },
        retry: { maxAttempts: Number($('#bj-tries').value) || 3, timeoutSeconds: Number($('#bj-timeout').value) || 900 },
        output: { onBadRecord: $('#bj-bad').value, spotCheck: Math.max(0, Number($('#bj-spot').value) || 0) },
      },
    };
  }

  function flash(sel, text, bad) { const el = $(sel); if (el) { el.textContent = text; el.classList.toggle('error', !!bad); } }

  async function plan() {
    let job;
    try { job = buildJob(); } catch (e) { flash('#bj-status', e.message, true); return; }
    flash('#bj-status', 'Checking…');
    try {
      const { plan: p } = await api('POST', '/api/engine/plan', { job });
      const box = $('#bj-plan');
      box.hidden = false;
      box.innerHTML = `<h2>${esc(DECISION[p.decision] || p.decision)}</h2><p>${esc((p.reasons || []).join(' '))}</p>
        <div class="scroll-x"><table class="res-table ag-table"><thead><tr><th>Computer</th><th>Can run it</th><th>Speed</th></tr></thead><tbody>
        ${(p.devices || []).map((d) => `<tr><td>${esc(d.name)}${d.owned ? '' : ' <span class="hint">(friend)</span>'}</td><td>${d.eligible ? '<span class="ag-badge ok">Yes</span>' : `<span class="ag-badge">No</span> <span class="hint">${esc(d.why || '')}</span>`}</td>
          <td>${d.eligible ? `${p.unit === 'items' ? `${d.rate.toFixed(1)} items/s` : `${bytes(d.rate)}/s`}${d.measured ? '' : ' (estimate)'}` : ''}</td></tr>`).join('')}</tbody></table></div>
        ${(p.setup || []).length ? `<p class="hint">Once you start it, the job waits for you to approve the setup. Each of these computers would run:</p>
          ${p.setup.map((s) => `<p><b>${esc(s.name)}</b></p><ol class="bj-steps">${s.steps.map((x) => `<li><code>${esc(x)}</code></li>`).join('')}</ol>`).join('')}
          <p class="hint">${esc(p.setup[0].downloads)}</p>` : ''}`;
      flash('#bj-status', '');
    } catch (e) { flash('#bj-status', e.message, true); }
  }

  async function start() {
    let job;
    try { job = buildJob(); } catch (e) { flash('#bj-status', e.message, true); return; }
    const btn = $('#bj-form button[type="submit"]');
    btn.disabled = true;
    flash('#bj-status', 'Starting…');
    try {
      const { job: j } = await api('POST', '/api/engine/jobs', { job });
      B.jobs.set(j.id, j);
      flash('#bj-status', 'Started. Follow it in Jobs.');
      renderJobs();
      showTab('jobs');
    } catch (e) { flash('#bj-status', e.message, true); } finally { btn.disabled = false; }
  }

  function showKind() {
    const kind = $('#bj-kind').value;
    const items = $('#bj-input').value === '__items__';
    document.querySelectorAll('#bj-form [data-for]').forEach((el) => { el.hidden = !el.dataset.for.split(' ').includes(kind); });
    $('#bj-items-field').hidden = !items;
    $('#bj-trust').disabled = CODE_KINDS.includes(kind);
    const best = $('#bj-reducer').value === 'best';
    document.querySelectorAll('#bj-form [data-reducer="best"]').forEach((el) => { el.hidden = !best; });
    const rt = $('#bj-runtime').value;
    document.querySelectorAll('#bj-form [data-runtime]').forEach((el) => { el.hidden = el.dataset.runtime !== rt; });
    $('#bj-rt-hint').textContent = rt === 'managed-python'
      ? 'Your code runs only on your own computers. Each one prepares this environment with uv the first time, after you approve it, and reuses it for later jobs that ask for the same Python and packages.'
      : 'Your code runs only on your own computers, with the Python YourHarness runs on and its packages (NumPy is there).';
  }

  function renderInputs() {
    const sel = $('#bj-input');
    if (!sel) return;
    const keep = sel.value;
    const opts = B.datasets.map((d) => [d.ref, `${d.name} (${d.format}, ${bytes(d.size)})`]);
    opts.push(['__items__', 'A list of items or parameters (your own function only)']);
    sel.innerHTML = opts.map(([v, t]) => `<option value="${esc(v)}">${esc(t)}</option>`).join('');
    if (opts.some(([v]) => v === keep)) sel.value = keep;
    const ms = $('#bj-model');
    if (ms) {
      const had = ms.value;
      ms.innerHTML = B.models.length ? B.models.map((m) => `<option value="${esc(m.ref)}">${esc(m.name)} (${esc(m.adapter)}, ${esc(m.sha.slice(0, 8))})</option>`).join('')
        : '<option value="">No models yet: add one in Datasets</option>';
      if (B.models.some((m) => m.ref === had)) ms.value = had;
    }
    showKind();
  }

  async function addModel() {
    const file = $('#bj-model-file').files && $('#bj-model-file').files[0];
    if (!file) { flash('#bj-model-status', 'Pick a model file first.', true); return; }
    const q = new URLSearchParams({ name: file.name });
    if ($('#bj-model-adapter').value) q.set('adapter', $('#bj-model-adapter').value);
    flash('#bj-model-status', `Uploading ${file.name} (${bytes(file.size)})…`);
    try {
      const { model } = await api('PUT', `/api/engine/models?${q}`, undefined, file);
      B.models = [model, ...B.models.filter((m) => m.id !== model.id)];
      flash('#bj-model-status', `Added ${model.name}: ${model.adapter}, ${bytes(model.size)}, SHA-256 ${model.sha.slice(0, 12)}….`);
      renderModels();
      renderInputs();
    } catch (e) { flash('#bj-model-status', e.message, true); }
  }

  function renderModels() {
    const box = $('#bj-models');
    if (!box) return;
    box.innerHTML = B.models.map((m) => `<section class="panel" data-model="${esc(m.id)}">
      <div class="panel-head"><h2>${esc(m.name)}</h2><div class="ag-acts"><button class="btn ghost small danger" type="button" data-act="delete-model" data-id="${esc(m.id)}">Delete</button></div></div>
      <p class="hint">${m.adapter === 'torchscript' ? 'TorchScript' : 'Python'} model · ${bytes(m.size)} · SHA-256 <code>${esc(m.sha)}</code>${m.files ? ` · ${nf.format(m.files.length)} files` : ''} · added ${esc(when(m.createdAt))} by ${esc(m.createdBy || '?')}</p></section>`).join('');
  }

  // ---------------------------------------------------------------- datasets
  async function loadImportable() {
    const sel = $('#bj-import');
    if (!sel) return;
    const opts = [['', 'None: use the file above']];
    try { (await api('GET', '/api/files')).files.forEach((f) => opts.push([`shared:${f.name}`, `Shared folder: ${f.name} (${bytes(f.size)})`])); } catch { /* not allowed or empty */ }
    try { (await api('GET', '/api/pool')).files.forEach((f) => opts.push([`pool:${f.name}`, `/pool/${f.name} (${bytes(f.size)})`])); } catch { /* no pooled storage */ }
    sel.innerHTML = opts.map(([v, t]) => `<option value="${esc(v)}">${esc(t)}</option>`).join('');
  }

  async function addDataset() {
    const file = $('#bj-file').files && $('#bj-file').files[0];
    const pick = $('#bj-import').value;
    const fmt = $('#bj-format').value;
    const cls = $('#bj-class').value;
    try {
      let ds;
      if (pick) {
        const [source, ...rest] = pick.split(':');
        flash('#bj-ds-status', 'Copying and indexing…');
        ds = (await api('POST', '/api/engine/datasets/import', { source, path: rest.join(':'), format: fmt || undefined, classification: cls })).dataset;
      } else if (file) {
        flash('#bj-ds-status', `Uploading ${file.name} (${bytes(file.size)})…`);
        const q = new URLSearchParams({ name: file.name, classification: cls });
        if (fmt) q.set('format', fmt);
        ds = (await api('PUT', `/api/engine/datasets?${q}`, undefined, file)).dataset;
      } else {
        flash('#bj-ds-status', 'Pick a file first.', true);
        return;
      }
      B.datasets = [ds, ...B.datasets.filter((d) => d.id !== ds.id)];
      flash('#bj-ds-status', `Added ${ds.name}: ${bytes(ds.size)}, ${nf.format(ds.lines || 0)} lines.`);
      renderDatasets();
      renderInputs();
    } catch (e) { flash('#bj-ds-status', e.message, true); }
  }

  function renderDatasets() {
    const box = $('#bj-datasets');
    if (!box) return;
    box.innerHTML = B.datasets.length ? B.datasets.map((d) => `<section class="panel" data-ds="${esc(d.id)}">
      <div class="panel-head"><h2>${esc(d.name)}</h2><div class="ag-acts">
        <button class="btn ghost small" type="button" data-act="preview" data-id="${esc(d.id)}">Preview</button>
        <button class="btn ghost small" type="button" data-act="share" data-id="${esc(d.id)}">${d.classification === 'shareable' ? 'Keep it on my computers' : 'Let friends process it'}</button>
        <button class="btn ghost small danger" type="button" data-act="delete-ds" data-id="${esc(d.id)}">Delete</button></div></div>
      <p class="hint">${esc(d.format)} · ${bytes(d.size)} · ${nf.format(d.lines || 0)} lines${d.header ? ` · columns ${esc(d.header.join(', '))}` : ''} · ${d.classification === 'shareable' ? 'friends may process it' : 'only your computers'} · added ${esc(when(d.createdAt))}</p>
      <pre class="ag-log" hidden></pre></section>`).join('')
      : '<p class="hint">No datasets yet.</p>';
  }

  // ---------------------------------------------------------------- computers
  function renderComputers() {
    const body = $('#bj-comp');
    if (!body) return;
    body.innerHTML = B.devices.map((d) => {
      const e = d.engine;
      const gpus = e && (e.accelerators || []).length ? `<br><span class="hint">GPU: ${esc(e.accelerators.map((a) => `${a.name}${a.memoryGB ? ` ${a.memoryGB} GB` : ''}`).join(', '))}</span>` : '';
      const envs = e && e.envs ? ` · ${nf.format(e.envs)} prepared environment${e.envs === 1 ? '' : 's'}` : '';
      return `<tr><td><b>${esc(d.name)}</b><br><span class="hint">${esc(d.kind)}</span></td><td>${d.owned ? 'Yours' : 'A friend’s'}</td>
        <td>${e ? e.slots : ''}</td><td>${e ? esc(e.ops.map((o) => (KINDS[o] || o).split(':')[0]).join(', ')) + `<br><span class="hint">${esc(e.formats.join(', '))} · ${esc(e.runtimes.join(', '))}${envs}</span>${gpus}`
          : `<span class="hint">${d.kind === 'agent' ? 'Runs a YourHarness without batch jobs: update it.' : 'Browser tabs don’t run batch jobs yet.'}</span>`}</td>
        <td>${e ? `${e.busy} of ${e.slots}` : ''}</td></tr>`;
    }).join('') || '<tr><td colspan="5" class="hint">No devices are connected.</td></tr>';
    if (B.storage) $('#bj-storage').textContent = `Stored on the coordinator: ${nf.format(B.storage.artifacts)} files, ${bytes(B.storage.bytes)}.`;
  }

  // ---------------------------------------------------------------- events
  async function act(fn) { try { await fn(); } catch (e) { alert(e.message); } }

  function onClick(e) {
    const tab = e.target.closest('[data-tab]');
    if (tab) { showTab(tab.dataset.tab); return; }
    const b = e.target.closest('[data-act]');
    if (!b) return;
    const id = b.dataset.id;
    switch (b.dataset.act) {
      case 'add-metric': addMetric(); break;
      case 'add-where': addWhere(); break;
      case 'remove-row': b.closest('.bj-row').remove(); break;
      case 'plan': plan(); break;
      case 'show-spec': {
        const pre = $('#bj-spec');
        try { pre.textContent = JSON.stringify(buildJob(), null, 2); pre.hidden = false; flash('#bj-status', 'Save this as a .json file to run it with: harness job run file.json'); } catch (err) { flash('#bj-status', err.message, true); }
        break;
      }
      case 'detail': showDetail(id); break;
      case 'close-detail': B.detail = null; $('#bj-detail').hidden = true; break;
      case 'cancel': if (confirm('Cancel this job?')) act(() => api('POST', `/api/engine/jobs/${encodeURIComponent(id)}/cancel`, {})); break;
      case 'retry': act(() => api('POST', `/api/engine/jobs/${encodeURIComponent(id)}/retry`, {})); break;
      case 'approve-setup': act(() => approveSetup(id)); break;
      case 'delete': if (confirm('Delete this job and its results?')) act(async () => { await api('DELETE', `/api/engine/jobs/${encodeURIComponent(id)}`); B.jobs.delete(id); renderJobs(); }); break;
      case 'add-dataset': addDataset(); break;
      case 'add-model': addModel(); break;
      case 'delete-model': if (confirm('Delete this model? Finished jobs keep its SHA-256 in their results.')) act(async () => { await api('DELETE', `/api/engine/models/${encodeURIComponent(id)}`); B.models = B.models.filter((m) => m.id !== id); renderModels(); renderInputs(); }); break;
      case 'preview': act(async () => {
        const pre = document.querySelector(`[data-ds="${CSS.escape(id)}"] pre`);
        const { records } = await api('GET', `/api/engine/datasets/${encodeURIComponent(id)}/preview`);
        pre.textContent = records.map((r) => JSON.stringify(r)).join('\n') || 'The dataset has no records.';
        pre.hidden = false;
      }); break;
      case 'share': act(async () => {
        const d = B.datasets.find((x) => x.id === id);
        const to = d.classification === 'shareable' ? 'owned-private' : 'shareable';
        if (to === 'shareable' && !confirm('Friends’ computers in the pool will be able to read this dataset when a job you start allows them. Continue?')) return;
        const { dataset } = await api('POST', `/api/engine/datasets/${encodeURIComponent(id)}`, { classification: to });
        B.datasets = B.datasets.map((x) => (x.id === id ? dataset : x));
        renderDatasets();
      }); break;
      case 'delete-ds': if (confirm('Delete this dataset?')) act(async () => { await api('DELETE', `/api/engine/datasets/${encodeURIComponent(id)}`); B.datasets = B.datasets.filter((x) => x.id !== id); renderDatasets(); renderInputs(); }); break;
      default: break;
    }
  }

  function onChange(e) {
    if (['bj-kind', 'bj-input', 'bj-reducer', 'bj-runtime'].includes(e.target.id)) showKind();
  }

  function renderAll() { renderJobs(); renderDatasets(); renderModels(); renderInputs(); renderComputers(); }

  window.addEventListener('yh:msg', (e) => {
    const m = e.detail || {};
    if (m.type === 'devices' && B.loaded) {
      const byId = new Map(B.devices.map((d) => [d.id, d]));
      B.devices = m.devices.map((d) => ({ id: d.id, name: d.name, kind: d.kind, owned: d.owned, engine: d.engine || (byId.get(d.id) || {}).engine }));
      renderComputers();
      return;
    }
    if (!B.loaded) return;
    if (m.type === 'eng.job' && m.job) {
      if (m.job.deleted) { B.jobs.delete(m.job.id); renderJobs(); return; }
      const had = B.jobs.has(m.job.id);
      B.jobs.set(m.job.id, m.job);
      if (had) renderJob(m.job.id); else renderJobs();
      if (B.detail === m.job.id && ['done', 'failed', 'cancelled'].includes(m.job.status)) showDetail(m.job.id);
    } else if (m.type === 'eng.datasets') {
      api('GET', '/api/engine').then((data) => { B.datasets = data.datasets; B.models = data.models || []; renderDatasets(); renderModels(); renderInputs(); }).catch(() => {});
    }
  });

  let built = false;
  window.YHBatch = {
    opened() {
      if (!built) { build(); built = true; showTab(B.tab); }
      load();
    },
  };
})();
