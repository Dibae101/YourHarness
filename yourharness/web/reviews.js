(() => {
  'use strict';
  const $ = (id) => document.getElementById(id);
  const base = '/api/reviews',
    tab = Math.random().toString(36).slice(2);
  const phases = {
    measuring: 'Measure',
    planned: 'Plan',
    rendering: 'Render',
    paused: 'Paused',
    encoding: 'Assemble',
    ready: 'Deliver',
    cancelled: 'Cancelled',
    failed: 'Assembly failed',
    fulfilled: 'Allowance completed',
  };
  const active = new Set(['measuring', 'rendering']);
  const ended = new Set(['ready', 'cancelled', 'failed', 'encoding', 'fulfilled']);
  let screen,
    job,
    invitation,
    session,
    invitationToken,
    run,
    poll,
    generation = 0,
    drawnStatus;
  const time = (sec) => new Date(sec * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  const bytes = (n) => (n < 1048576 ? Math.ceil(n / 1024) + ' KiB' : (n / 1048576).toFixed(1) + ' MiB');
  const duration = (s) => (s < 60 ? Math.ceil(s) + ' sec' : Math.ceil(s / 60) + ' min');
  function note(msg = '') {
    $('notice').textContent = msg;
    $('notice').hidden = !msg;
  }
  function show(id) {
    screen = id;
    for (const key of ['library', 'compose', 'detail', 'lender']) $(key).hidden = key !== id;
  }
  function el(tag, text, cls) {
    const node = document.createElement(tag);
    node.textContent = text;
    if (cls) node.className = cls;
    return node;
  }
  function button(text, fn, cls) {
    const b = el('button', text, cls);
    b.type = 'button';
    b.onclick = () => guard(b, fn);
    return b;
  }
  async function guard(control, fn) {
    if (control.disabled) return;
    control.disabled = true;
    note();
    try {
      await fn();
    } catch (e) {
      note(e.message);
    } finally {
      control.disabled = false;
    }
  }
  async function api(path = '', opts = {}) {
    const headers = { 'X-Review-Tab': tab, ...opts.headers };
    if (opts.json) {
      opts.body = JSON.stringify(opts.json);
      headers['Content-Type'] = 'application/json';
    }
    const res = await fetch(base + path, {
      method: opts.method || 'GET',
      body: opts.body,
      headers,
      signal: opts.signal,
      keepalive: opts.keepalive,
      cache: 'no-store',
    });
    if (!res.ok) {
      let message;
      try {
        message = (await res.json()).error;
      } catch (_) {}
      const err = new Error(message || 'Request failed (' + res.status + ').');
      err.status = res.status;
      throw err;
    }
    return opts.binary ? res.arrayBuffer() : res.json();
  }
  const secretHeaders = (token) => (token ? { 'X-Review-Session': token } : {});
  function stopRun(end = false) {
    const r = run;
    if (!r) return;
    run = null;
    r.controller.abort();
    for (const w of r.workers) w.terminate();
    void api('/' + r.id + '/release' + (end ? '?end=1' : ''), {
      method: 'POST',
      headers: secretHeaders(r.session),
      keepalive: true,
    }).catch(() => {});
  }
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  function render(worker, payload, signal) {
    return new Promise((resolve, reject) => {
      const abort = () => finish(reject, new Error('Stopped'));
      const timer = setTimeout(
        () =>
          finish(reject, new Error('This frame took too long. Reduce the preview size or simplify the scene.')),
        75000,
      );
      function finish(callback, value) {
        clearTimeout(timer);
        signal.removeEventListener('abort', abort);
        worker.onmessage = worker.onerror = null;
        callback(value);
      }
      worker.onmessage = (e) =>
        e.data.error ? finish(reject, new Error(e.data.error)) : finish(resolve, e.data.frame);
      worker.onerror = () =>
        finish(reject, new Error('The render worker failed. Use a browser with OffscreenCanvas support.'));
      signal.addEventListener('abort', abort, { once: true });
      worker.postMessage(payload, [payload.input]);
    });
  }
  async function lane(r) {
    const apiPath = '/' + r.id,
      headers = secretHeaders(r.session),
      signal = r.controller.signal;
    const w = new Worker('/review-worker.js');
    r.workers.push(w);
    try {
      while (!signal.aborted) {
        const part = await api(apiPath + '/claim', { method: 'POST', headers, signal });
        if (r.session)
          $('loan-progress').textContent =
            phases[part.status] +
            ' · ' +
            part.done +
            ' of ' +
            part.total +
            ' project frames complete. Your tab contributed ' +
            r.completed +
            '.';
        if (ended.has(part.status) || (!r.session && !active.has(part.status))) {
          if (r.session) finishLoan('Lending finished. ' + r.completed + ' frames contributed by this tab.');
          return;
        }
        if (!part.ticket) {
          await sleep(550);
          continue;
        }
        const path = apiPath + '/lease/' + part.ticket;
        try {
          const input = await api(path, { headers, signal, binary: true });
          const t = performance.now();
          const frame = await render(w, { ...part, input }, signal);
          await api(path, {
            method: 'PUT',
            headers: {
              ...headers,
              'X-Render-Seconds': String(Math.max(0.001, (performance.now() - t) / 1000)),
            },
            body: frame,
            signal,
          });
          r.completed++;
        } catch (e) {
          if (e.status !== 409) throw e;
        }
      }
    } catch (e) {
      if (!signal.aborted) {
        if (r.session) finishLoan('Lending stopped. ' + e.message);
        else {
          stopRun();
          ownerWork = false;
          note(e.message + ' Use Resume this browser to try again.');
        }
      }
    } finally {
      w.terminate();
      r.finished++;
      if (run === r && r.finished === r.count) run = null;
    }
  }
  let ownerWork = true,
    expiryTimer,
    monitorTimer;
  function startRun(id, token, count) {
    if (run || document.hidden) return;
    if (typeof Worker === 'undefined' || typeof OffscreenCanvas === 'undefined') {
      throw new Error('Rendering needs a current browser with OffscreenCanvas support.');
    }
    const r = {
      id,
      session: token,
      controller: new AbortController(),
      workers: [],
      completed: 0,
      finished: 0,
      count,
    };
    run = r;
    for (let i = 0; i < count; i++) void lane(r);
  }
  function finishLoan(message) {
    clearTimeout(expiryTimer);
    clearInterval(monitorTimer);
    stopRun(true);
    session = null;
    $('accept').hidden = true;
    $('loan-active').hidden = false;
    $('loan-progress').textContent = message;
    $('stop-loan').hidden = true;
  }
  function sourceHelp() {
    const kind = $('source').value,
      file = $('files');
    $('files-field').hidden = kind === 'demo';
    $('exporter').hidden = kind !== 'mesh';
    $('rate-field').hidden = kind !== 'images';
    file.required = kind !== 'demo';
    file.multiple = kind === 'images';
    file.accept = kind === 'mesh' ? '.yhreview' : 'image/jpeg,image/png';
    file.value = '';
    $('source-help').textContent = {
      demo: 'A 72-frame turntable. No files or extra software required.',
      images:
        'Choose JPEG or PNG files. Frames play in filename order. All files combined must fit in 128 MiB.',
      mesh: 'In Blender, install the exporter from disk, then use File → Export → YourHarness motion review. Upload the .yhreview file here.',
    }[kind];
    if (kind === 'demo' && !$('title').value) $('title').value = 'Turntable review';
  }
  function newReview() {
    show('compose');
    note();
    $('create').reset();
    sourceHelp();
    const d = new Date(Date.now() + 3600000);
    d.setMinutes(d.getMinutes() - d.getTimezoneOffset());
    $('deadline').value = d.toISOString().slice(0, 16);
  }
  async function create() {
    const kind = $('source').value;
    const params = new URLSearchParams({
      kind,
      name: $('title').value.trim(),
      deadline: Date.parse($('deadline').value) / 1000,
      width: $('quality').value,
      fps: $('fps').value,
    });
    const body = new FormData();
    const files = Array.from($('files').files).sort((a, b) =>
      a.name.localeCompare(b.name, undefined, { numeric: true }),
    );
    if (files.reduce((a, f) => a + f.size, 0) > 128 * 1024 ** 2)
      throw new Error('Select at most 128 MiB of source files.');
    for (const f of files) body.append('file', f, f.name);
    const { id } = await api('?' + params, { method: 'POST', body });
    location.hash = 'job=' + id;
  }
  async function list() {
    const current = generation,
      all = await api();
    if (current !== generation) return;
    $('jobs').replaceChildren();
    if (!all.length) {
      const empty = el('div', '', 'empty');
      empty.append(
        el('h2', 'Create your first review'),
        el('p', 'Try the built-in turntable, import an image sequence, or bring a Blender motion review.'),
        button(
          'Try the demo',
          () => {
            newReview();
            $('source').value = 'demo';
            sourceHelp();
          },
          'primary',
        ),
      );
      $('jobs').append(empty);
    }
    for (const j of all) {
      const card = button(
        '',
        () => {
          location.hash = 'job=' + j.id;
        },
        'review-card',
      );
      if (j.preview) {
        const im = new Image();
        im.src = base + '/' + j.id + '/poster';
        im.alt = '';
        card.append(im);
      }
      card.append(
        el('small', phases[j.status].toUpperCase()),
        el('strong', j.name),
        el('span', j.done + ' / ' + j.total + ' frames · ' + time(j.deadline)),
      );
      $('jobs').append(card);
    }
  }
  function action(label, action, cls) {
    return button(
      label,
      async () => {
        await api('/' + job.id + '/action', { method: 'POST', json: { action } });
        if (action === 'start') ownerWork = true;
        await refresh();
      },
      cls,
    );
  }
  function draw(j, p) {
    job = j;
    const path = base + '/' + j.id;
    $('job-name').textContent = j.name;
    $('job-meta').textContent =
      j.w +
      ' × ' +
      j.h +
      ' · ' +
      Number(j.fps.toFixed(2)) +
      ' fps · ' +
      bytes(j.bytes) +
      ' inputs · Due ' +
      new Date(j.deadline * 1000).toLocaleString();
    $('state').textContent = phases[j.status];
    $('progress').max = j.total;
    $('progress').value = j.done;
    $('count').textContent = j.done + ' / ' + j.total;
    $('timing').textContent =
      p.localSeconds == null
        ? 'Measuring representative frames on this device.'
        : 'One local worker · ' +
          duration(p.localRange[0]) +
          '–' +
          duration(p.localRange[1]) +
          ' remaining before assembly (sample-based).';
    if (p.localSeconds != null && active.has(j.status) && p.assemblySeconds != null)
      $('timing').textContent +=
        ' Estimated playback time ' +
        time(Date.now() / 1000 + p.localSeconds + p.assemblySeconds) +
        ' using recent assembly times.';
    if (j.status === 'ready' && j.finished_at)
      $('timing').textContent =
        'Playable at ' +
        time(j.finished_at) +
        ' · ' +
        duration(j.finished_at - j.created) +
        ' after import · ' +
        (j.finished_at <= j.deadline ? 'met the deadline.' : 'finished after the deadline.');
    if (j.status === 'encoding') $('timing').textContent = 'All frames are in. Assembling the silent MP4.';
    if (j.status === 'cancelled') $('timing').textContent = 'Rendering and lending have stopped.';
    if (j.status === 'paused')
      $('timing').textContent = 'Accepted frames are saved. Resume when you are ready.';
    const assets = $('assets');
    if (assets.dataset.job !== j.id) {
      assets.dataset.job = j.id;
      assets.replaceChildren(...j.sources.map((name) => el('li', name)));
    }
    const ready = j.status === 'ready';
    $('detail').classList.toggle('closed', ended.has(j.status));
    $('movie').hidden = !ready;
    $('still').hidden = ready || !j.preview;
    $('screen-msg').hidden = ready || j.preview;
    if (ready && $('movie').getAttribute('src') !== path + '/clip') $('movie').src = path + '/clip';
    if (j.preview && $('still').getAttribute('src') !== path + '/poster') $('still').src = path + '/poster';
    $('job-error').hidden = !j.error;
    $('job-error').textContent = j.error;
    $('invite-box').hidden = ended.has(j.status);
    $('plan-box').hidden = ended.has(j.status);
    $('scope').textContent =
      'The helper can see assigned ' +
      (j.kind === 'mesh' ? 'geometry, colors and camera motion' : 'images') +
      '. Up to ' +
      bytes(j.bytes) +
      ' of frame inputs may be shared.';
    $('invite-frames').max = j.total;
    if (!$('invite-frames').value) $('invite-frames').value = Math.max(1, j.total - j.done);
    $('contributors').replaceChildren(
      ...j.contributors.map((c) => el('div', c.label + ' · ' + c.frames + ' frames', 'entry')),
    );
    if (!j.contributors.length) $('contributors').textContent = 'No frames accepted yet.';
    const phase = { measuring: 0, planned: 1, rendering: 2, paused: 2, encoding: 3, ready: 3, failed: 3 }[
      j.status
    ];
    Array.from($('steps').children).forEach((step, i) => {
      step.className = i === phase ? 'current' : i < phase ? 'complete' : '';
      if (i === phase) step.setAttribute('aria-current', 'step');
      else step.removeAttribute('aria-current');
    });
    const profiles = p.profiles || [];
    const select = $('profiles'),
      optionsKey = JSON.stringify(profiles);
    if (select.dataset.key !== optionsKey) {
      const selected = select.value;
      select.replaceChildren(el('option', 'Enter assumptions'));
      profiles.forEach((profile, i) => {
        const opt = el('option', profile.label + ' · ' + profile.project + ' (' + profile.frames + ' frames)');
        opt.value = String(i);
        select.append(opt);
      });
      if ([...select.options].some((o) => o.value === selected)) select.value = selected;
      select.dataset.key = optionsKey;
      select.onchange = () => {
        const profile = profiles[select.value];
        if (profile) {
          $('helper-speed').value = Math.max(0.001, profile.compute).toFixed(3);
          $('bandwidth').value = Math.max(0.001, profile.mbps).toFixed(3);
        }
      };
    }
    $('profile-field').hidden = !profiles.length;
    const loans = j.loans || [];
    const key = JSON.stringify(
      loans.map((l) => [l.id, l.done, l.stopped, l.accepted, l.expires < Date.now() / 1000]),
    );
    if ($('loans').dataset.key !== key) {
      $('loans').dataset.key = key;
      $('loans').replaceChildren(
        ...loans.map((l) => {
          const state = l.stopped
            ? 'Ended'
            : l.done >= l.quota
              ? 'Completed'
              : l.expires < Date.now() / 1000
                ? 'Expired'
                : l.accepted
                  ? 'Lending'
                  : 'Awaiting acceptance';
          const row = el('div', '', 'entry');
          row.append(
            el('div', (l.label || 'Guest invitation') + ' · ' + state),
            el('span', l.done + ' / ' + l.quota + ' frames · ends ' + time(l.expires)),
          );
          if (state === 'Lending' || state === 'Awaiting acceptance')
            row.append(
              button(
                'Revoke',
                async () => {
                  await api('/' + j.id + '/action', {
                    method: 'POST',
                    json: { action: 'revoke', grant: l.id },
                  });
                  await refresh();
                },
                'text-btn',
              ),
            );
          return row;
        }),
      );
    }
    $('revoke').hidden = !loans.some((l) => !l.stopped && l.expires > Date.now() / 1000 && l.done < l.quota);
    const signature = j.status + ':' + ownerWork;
    if (drawnStatus !== signature) {
      const a = $('actions');
      a.replaceChildren();
      drawnStatus = signature;
      if (['planned', 'paused', 'measuring'].includes(j.status))
        a.append(action(j.status === 'paused' ? 'Resume review' : 'Start full review', 'start', 'primary'));
      if (active.has(j.status)) a.append(action('Pause review', 'pause'));
      if (active.has(j.status) && !ownerWork)
        a.append(
          button('Resume this browser', () => {
            ownerWork = true;
            return refresh();
          }),
        );
      if (j.status === 'failed') a.append(action('Retry MP4 assembly', 'retry', 'primary'));
      if (!ended.has(j.status) || j.status === 'failed') a.append(action('Cancel review', 'cancel'));
      if (['ready', 'cancelled', 'failed'].includes(j.status))
        a.append(
          button('Delete review', async () => {
            if (!window.confirm('Delete this review, its inputs and its video from the pool?')) return;
            await api('/' + j.id, { method: 'DELETE' });
            location.hash = '';
          }),
        );
      if (ready) {
        const link = el('a', 'Download MP4', 'action-link');
        link.href = path + '/clip';
        link.download = j.name + '.mp4';
        a.append(link);
      }
    }
    if (active.has(j.status) && ownerWork) startRun(j.id, null, 1);
    else stopRun();
  }
  async function refresh() {
    if (screen !== 'detail' || !job) return;
    const current = generation,
      id = job.id;
    const [j, p] = await Promise.all([api('/' + id), api('/' + id + '/plan')]);
    if (current === generation && screen === 'detail') draw(j, p);
  }
  async function compare() {
    const p = await api(
      '/' +
        job.id +
        '/plan?' +
        new URLSearchParams({ helper: $('helper-speed').value, mbps: $('bandwidth').value }),
    );
    if (p.combinedSeconds == null) {
      $('projection').textContent = 'Finish measuring a local frame first.';
      return;
    }
    $('projection').textContent =
      (p.helperFrames
        ? 'Assign ' +
          p.helperFrames +
          ' frames. Estimated finish ' +
          time(Date.now() / 1000 + p.combinedSeconds + (p.assemblySeconds || 0)) +
          ', including transfer.'
        : 'Keep it local. No helper frames needed for this plan.') +
      ' ' +
      (p.meetsDeadline ? 'Within the requested frame budget.' : 'The estimate runs past the deadline.') +
      (p.assemblySeconds == null
        ? ' MP4 assembly has not been timed yet.'
        : ' Includes estimated MP4 assembly.');
    if (p.helperFrames) {
      $('invite-frames').value = p.helperFrames;
      $('invite-min').value = Math.max(1, Math.min(1440, p.helperMinutes));
    }
  }
  async function invite() {
    const g = await api('/' + job.id + '/invites', {
      method: 'POST',
      json: {
        minutes: +$('invite-min').value,
        workers: +$('invite-slots').value,
        frames: +$('invite-frames').value,
      },
    });
    $('loan-link').value = g.url || location.origin + '/reviews.html#invite=' + g.token;
    $('link-panel').hidden = false;
    $('loan-link').select();
  }
  async function lender() {
    $('workspace').hidden = true;
    show('lender');
    invitation = await api('/invitation', { headers: { 'X-Review-Invite': invitationToken } });
    const i = invitation;
    $('loan-name').textContent = i.name;
    $('loan-info').textContent =
      'One review · up to ' + i.quota + ' frames · invitation ends ' + time(i.expires);
    $('terms').textContent =
      'Your browser will receive assigned ' +
      (i.kind === 'mesh' ? 'projected geometry' : 'images') +
      ' and return rendered frames. Maximum project input ' +
      bytes(i.bytes) +
      '. Frame size ' +
      i.w +
      ' × ' +
      i.h +
      '.';
    $('slots').replaceChildren(...Array.from({ length: i.workers }, (_, k) => el('option', String(k + 1))));
    $('minutes').max = Math.max(1, Math.ceil((i.expires - Date.now() / 1000) / 60));
    $('minutes').value = Math.min(15, +$('minutes').max);
    $('accept').hidden = false;
    $('loan-active').hidden = true;
  }
  async function accept() {
    if (typeof Worker === 'undefined' || typeof OffscreenCanvas === 'undefined')
      throw new Error('Open this invite in a browser with OffscreenCanvas support.');
    const perWorker = invitation.kind === 'images' ? 128 : 24;
    const workers = Math.min(+$('slots').value, Math.floor(+$('memory').value / perWorker));
    session = await api('/accept', {
      method: 'POST',
      headers: { 'X-Review-Invite': invitationToken },
      json: { name: $('lender-name').value, workers, minutes: +$('minutes').value },
    });
    $('accept').hidden = true;
    $('loan-active').hidden = false;
    $('stop-loan').hidden = false;
    clearTimeout(expiryTimer);
    expiryTimer = setTimeout(
      () => finishLoan('Your selected lending time has ended.'),
      Math.max(0, session.expires * 1000 - Date.now()),
    );
    const loan = session;
    monitorTimer = setInterval(async () => {
      try {
        const state = await api('/' + loan.id + '/loan', { headers: secretHeaders(loan.session) });
        if (session === loan && ended.has(state.status)) finishLoan('This review has finished rendering.');
      } catch (e) {
        if (session === loan && e.status === 403) finishLoan('The owner ended this loan or its time expired.');
      }
    }, 1800);
    startRun(session.id, session.session, session.workers);
  }
  async function route() {
    generation++;
    clearTimeout(expiryTimer);
    clearInterval(monitorTimer);
    stopRun(!!session);
    session = null;
    clearTimeout(poll);
    note();
    job = null;
    drawnStatus = null;
    $('workspace').hidden = false;
    $('link-panel').hidden = true;
    $('invite-frames').value = '';
    $('projection').textContent = '';
    $('movie').removeAttribute('src');
    $('movie').load();
    $('still').removeAttribute('src');
    $('accept').hidden = true;
    const h = new URLSearchParams(location.hash.slice(1));
    invitationToken = h.get('invite');
    try {
      if (invitationToken) {
        await lender();
        return;
      }
      if (!h.has('job')) {
        show('library');
        await list();
        return;
      }
      show('detail');
      job = { id: h.get('job') };
      ownerWork = true;
      const current = generation;
      async function tick() {
        try {
          await refresh();
        } catch (e) {
          if (current === generation) note(e.message);
        }
        if (current === generation) poll = setTimeout(tick, 1200);
      }
      await tick();
    } catch (e) {
      note(e.status === 401 ? 'Sign in to your pool using the workspace link above.' : e.message);
    }
  }
  function form(id, fn) {
    $(id).onsubmit = (e) => {
      e.preventDefault();
      void guard(e.submitter || $(id).querySelector('button'), fn);
    };
  }
  $('new').onclick = newReview;
  $('source').onchange = sourceHelp;
  for (const b of document.querySelectorAll('[data-back]'))
    b.onclick = () => {
      if (location.hash) location.hash = '';
      else void route();
    };
  form('create', create);
  form('plan-form', compare);
  form('invite-form', invite);
  form('accept', accept);
  $('stop-loan').onclick = () =>
    finishLoan('You stopped lending. Unfinished frames are available to the other workers.');
  $('revoke').onclick = () =>
    guard($('revoke'), async () => {
      await api('/' + job.id + '/action', { method: 'POST', json: { action: 'revoke' } });
      $('link-panel').hidden = true;
      note('All links and active loans for this review have ended.');
    });
  $('copy-link').onclick = async () => {
    try {
      await navigator.clipboard.writeText($('loan-link').value);
      note('Invitation copied.');
    } catch (_) {
      $('loan-link').select();
      note('Select and copy the invitation.');
    }
  };
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      if (session) finishLoan('Lending ended when this tab was hidden.');
      else stopRun();
    } else if (screen === 'detail') void refresh().catch((e) => note(e.message));
  });
  window.addEventListener('pagehide', () => stopRun(!!session));
  window.addEventListener('hashchange', route);
  void route();
})();
