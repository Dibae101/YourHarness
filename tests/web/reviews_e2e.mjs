// Real Chromium, a disposable coordinator, two separate browser identities and an encoded MP4.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { chromium } from 'playwright';

const tmp = await mkdtemp(join(tmpdir(), 'yh-review-ui-'));
const child = spawn(
  process.env.YH_PYTHON || 'python3',
  [
    '-m',
    'yourharness',
    'serve',
    '--host',
    '127.0.0.1',
    '--port',
    '0',
    '--no-qr',
    '--no-local-agent',
    '--data-dir',
    join(tmp, 'data'),
    '--shared-dir',
    join(tmp, 'shared'),
  ],
  { env: { ...process.env, PYTHONUNBUFFERED: '1' }, stdio: ['ignore', 'pipe', 'pipe'] },
);
let output = '';
child.stdout.on('data', (b) => (output += b));
child.stderr.on('data', (b) => (output += b));
let browser;
async function until(fn, ms = 30000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const result = await fn();
    if (result) return result;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error('Timed out waiting for ' + fn);
}
try {
  const port = await until(() => output.match(/Listening on port (\d+)/)?.[1]);
  const base = 'http://127.0.0.1:' + port;
  const config = JSON.parse(await readFile(join(tmp, 'data', 'coordinator.json'), 'utf8'));
  browser = await chromium.launch({
    headless: true,
    ...(process.env.YH_CHROME ? { executablePath: process.env.YH_CHROME } : {}),
  });
  const owner = await browser.newContext(),
    helper = await browser.newContext();
  const login = await owner.request.post(base + '/api/join', {
    data: { token: config.join_token, name: 'Artist' },
  });
  assert.equal(login.status(), 200);
  const artist = await owner.newPage(),
    friend = await helper.newPage();
  const errors = [];
  for (const p of [artist, friend]) p.on('pageerror', (e) => errors.push(e.message));
  await artist.goto(base + '/');
  await artist.locator('#review-dock-item').click();
  await artist.locator('#new').click();
  await artist.selectOption('#source', 'demo');
  await artist.locator('#create button[type=submit]').click();
  await until(async () => (await artist.locator('#state').textContent()) === 'Plan');
  await artist.locator('#helper-speed').fill('0.02');
  await artist.locator('#bandwidth').fill('20');
  await artist.locator('#plan-form button').click();
  await until(() => artist.locator('#projection').textContent());
  await artist.locator('#invite-frames').fill('40');
  await artist.locator('#invite-form button').click();
  await artist.locator('#loan-link').waitFor();
  const invitation = await artist.locator('#loan-link').inputValue();
  assert(invitation.includes('#invite='));
  await friend.goto(base + '/reviews.html#' + invitation.split('#')[1]);
  await friend.locator('#lender-name').fill('Teammate');
  await friend.locator('#accept button').click();
  await friend.locator('#stop-loan').waitFor();
  // Slow only the artist's uploads to make reclaim deterministic.
  await artist.route('**/lease/*', async (route) => {
    if (route.request().method() === 'PUT') await new Promise((r) => setTimeout(r, 80));
    await route.continue().catch(() => {});
  });
  await artist.getByRole('button', { name: 'Start full review' }).click();
  await until(async () => /contributed [1-9]/.test(await friend.locator('#loan-progress').textContent()));
  await friend.locator('#stop-loan').click();
  await until(async () => (await artist.locator('#state').textContent()) === 'Deliver', 60000);
  assert.match(await artist.locator('#contributors').textContent(), /Teammate/);
  const id = artist.url().split('#job=')[1];
  const video = await owner.request.get(base + '/api/reviews/' + id + '/clip');
  assert.equal(video.status(), 200);
  assert.equal((await video.body()).toString('ascii', 4, 8), 'ftyp');
  assert.equal(await artist.locator('#count').textContent(), '72 / 72');
  assert.equal((await helper.request.get(base + '/api/reviews')).status(), 401);
  await artist.locator('#movie').evaluate(async (video) => {
    await video.play();
    video.pause();
  });
  if (process.env.YH_SCREENSHOTS) {
    await mkdir(process.env.YH_SCREENSHOTS, { recursive: true });
    await artist.screenshot({ path: join(process.env.YH_SCREENSHOTS, 'delivery.png'), fullPage: true });
    await artist.setViewportSize({ width: 390, height: 844 });
    assert(await artist.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    await artist.screenshot({ path: join(process.env.YH_SCREENSHOTS, 'mobile.png'), fullPage: true });
  }
  await artist.unrouteAll({ behavior: 'wait' });
  for (const kind of ['images', ...(process.env.YH_BLENDER_PACKAGE ? ['mesh'] : [])]) {
    if (!(await artist.locator('#library').isVisible())) await artist.locator('[data-back]:visible').click();
    await artist.locator('#new').click();
    await artist.selectOption('#source', kind);
    await artist.locator('#title').fill(kind + ' import');
    if (kind === 'mesh') await artist.locator('#files').setInputFiles(process.env.YH_BLENDER_PACKAGE);
    else {
      const colors = await artist.evaluate(() => {
        const canvas = document.createElement('canvas');
        canvas.width = canvas.height = 16;
        const ctx = canvas.getContext('2d');
        return ['red', 'lime', 'blue'].map((color) => {
          ctx.fillStyle = color;
          ctx.fillRect(0, 0, 16, 16);
          return canvas.toDataURL().split(',')[1];
        });
      });
      const images = [2, 0, 1].map((i) => ({
        name: ['1.png', '2.png', '10.png'][i],
        mimeType: 'image/png',
        buffer: Buffer.from(colors[i], 'base64'),
      }));
      await artist.locator('#files').setInputFiles(images);
      await artist.locator('#fps').fill('3');
    }
    await artist.locator('#create button[type=submit]').click();
    await until(
      async () =>
        (await artist.locator('#state').textContent()) === 'Plan' &&
        (await artist.locator('#detail').isVisible()),
    );
    await artist.getByRole('button', { name: 'Start full review' }).click();
    await until(async () => (await artist.locator('#state').textContent()) === 'Deliver');
    assert(await artist.locator('#movie').isVisible());
    if (kind === 'images') {
      const channels = await artist.locator('#movie').evaluate(async (video) => {
        await video.play();
        video.pause();
        const canvas = document.createElement('canvas');
        canvas.width = canvas.height = 1;
        const ctx = canvas.getContext('2d', { willReadFrequently: true }),
          result = [];
        for (const sec of [0.1, 0.45, 0.85]) {
          await new Promise((resolve) => {
            video.addEventListener('seeked', resolve, { once: true });
            video.currentTime = sec;
          });
          ctx.drawImage(video, 0, 0, 1, 1);
          const rgb = [...ctx.getImageData(0, 0, 1, 1).data].slice(0, 3);
          result.push(rgb.indexOf(Math.max(...rgb)));
        }
        return result;
      });
      assert.deepEqual(channels, [0, 1, 2], 'MP4 frames must follow numeric filename order');
    }
    artist.once('dialog', (dialog) => dialog.accept());
    await artist.getByRole('button', { name: 'Delete review' }).click();
    await artist.locator('#library').waitFor();
  }
  assert.deepEqual(errors, []);
  console.log(
    'Review UI passed: measure, plan, invite, reclaim, contributors, MP4 playback and source imports.',
  );
} finally {
  if (browser) await browser.close();
  const stopped = new Promise((resolve) => child.once('exit', resolve));
  if (child.exitCode === null) {
    child.kill();
    await stopped;
  }
  await rm(tmp, { recursive: true, force: true });
}
