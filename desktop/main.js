// YourHarness desktop app (Mac and Windows).
// On launch it looks for a pool on the network. If one answers, you enter its 4-digit code once;
// if none does, it starts a pool on this computer. Either way this computer shares in the
// background (menu bar or tray) and the workspace opens in this window.
const { app, BrowserWindow, Tray, Menu, ipcMain, shell, nativeImage, clipboard } = require('electron');
const MAC = process.platform === 'darwin';
const { spawn, execFile } = require('child_process');
const dgram = require('dgram');
const http = require('http');
const path = require('path');
const fs = require('fs');
const os = require('os');

const BEACON_PORT = Number(process.env.YH_BEACON_PORT || 8421);
const OWN_PORT = Number(process.env.YH_PORT || 8420);
const WIN = process.platform === 'win32';

if (process.env.YH_DEBUG_PORT) app.commandLine.appendSwitch('remote-debugging-port', process.env.YH_DEBUG_PORT);
if (!app.requestSingleInstanceLock()) { app.quit(); process.exit(0); }

let win = null;
let tray = null;
let quitting = false;
let poolUrl = null; // the address of the pool this window shows
let ownPort = null; // the port of the pool this app started on this computer, if any
const children = new Set();

const dataDir = () => app.getPath('userData');
const cfgPath = () => path.join(dataDir(), 'config.json');
const readCfg = () => { try { return JSON.parse(fs.readFileSync(cfgPath(), 'utf8')); } catch { return {}; } };
const writeCfg = (c) => { fs.mkdirSync(dataDir(), { recursive: true }); fs.writeFileSync(cfgPath(), JSON.stringify(c)); };
const log = (...a) => { try { fs.appendFileSync(path.join(dataDir(), 'app.log'), `${new Date().toISOString()} ${a.join(' ')}\n`); } catch { /* no log */ } };

// ---------------------------------------------------------------- backend program
function backendPath() {
  const exe = WIN ? 'harness.exe' : 'harness';
  const candidates = [
    process.env.YH_BACKEND,
    path.join(process.resourcesPath || '', 'backend', exe),
    path.join(__dirname, '..', 'build', 'dist', 'backend', exe),
  ].filter(Boolean);
  return candidates.find((p) => fs.existsSync(p));
}

function run(args, env = {}) {
  const exe = backendPath();
  if (!exe) throw new Error('The YourHarness engine is missing from this app. Reinstall it.');
  const child = spawn(exe, args, {
    windowsHide: true,
    env: { ...process.env, ...env, YOURHARNESS_HOME: path.join(dataDir(), 'agent'), PYTHONUNBUFFERED: '1' },
  });
  children.add(child);
  child.on('exit', () => children.delete(child));
  const tail = [];
  const feed = (d) => { for (const line of String(d).split(/\r?\n/)) if (line) { tail.push(line); if (tail.length > 40) tail.shift(); log(args[0], line); } };
  child.stdout.on('data', feed);
  child.stderr.on('data', feed);
  child.tail = tail;
  return child;
}

function waitFor(child, regex, ms = 20000) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => done(new Error('It took too long to start.')), ms);
    const check = (d) => { const m = regex.exec(String(d)); if (m) done(null, m); };
    const onExit = (code) => done(new Error(lastMessage(child) || `It stopped (code ${code}).`));
    function done(err, m) { clearTimeout(t); child.stdout.off('data', check); child.off('exit', onExit); err ? reject(err) : resolve(m); }
    child.stdout.on('data', check);
    child.on('exit', onExit);
  });
}
const lastMessage = (child) => child.tail.filter((l) => !/^\d\d:\d\d:\d\d/.test(l)).slice(-2).join(' ');

function killOne(c) {
  try { WIN ? execFile('taskkill', ['/pid', String(c.pid), '/T', '/F']) : c.kill('SIGTERM'); } catch { /* gone */ }
}
function killAll(except = null) {
  for (const c of [...children]) if (c !== except) killOne(c);
}

// ---------------------------------------------------------------- finding a pool
function discover(ms = 1500) {
  return new Promise((resolve) => {
    const found = new Map();
    const sock = dgram.createSocket('udp4');
    sock.on('message', (msg, rinfo) => {
      try {
        const info = JSON.parse(String(msg));
        if (info.app === 'yourharness') found.set(`${rinfo.address}:${info.port}`, { host: rinfo.address, port: info.port, name: info.name || '' });
      } catch { /* not ours */ }
    });
    sock.on('error', () => resolve([]));
    sock.bind(0, () => {
      sock.setBroadcast(true);
      for (const t of ['255.255.255.255', '127.0.0.1']) sock.send('YOURHARNESS?', BEACON_PORT, t, () => {});
      setTimeout(() => {
        try { sock.close(); } catch { /* closed */ }
        const mine = localAddresses();
        // this app's own pool is not something to join, and the same pool can answer on several addresses
        resolve([...found.values()].filter((f) => !(mine.has(f.host) && f.port === ownPort)));
      }, ms);
    });
  });
}

// Resolves { ok: true } or { ok: false, reason } so the screen can say what to try.
function check(url) {
  return new Promise((resolve) => {
    const req = http.get(url + '/install.sh', { timeout: 4000 }, (res) => { res.resume(); resolve({ ok: res.statusCode === 200, reason: `It answered with status ${res.statusCode}, so it may not be a YourHarness pool.` }); });
    req.on('error', (e) => resolve({ ok: false, reason: e.code || e.message }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, reason: 'ETIMEDOUT' }); });
  });
}
const reachable = async (url) => (await check(url)).ok;

function explain(host, port, reason) {
  const where = `${host}:${port}`;
  const macHint = MAC ? ' On a Mac, open System Settings → Privacy & Security → Local Network and turn on YourHarness, then try again.' : '';
  if (reason === 'ECONNREFUSED') return `${where} is reachable but no pool is listening on that port. Check the port number in the pool's "Add a device" panel.`;
  if (reason === 'EHOSTUNREACH' || reason === 'ENETUNREACH' || reason === 'EPERM' || reason === 'EACCES') return `This computer is not allowed to reach ${where}.${macHint}`;
  if (reason === 'ETIMEDOUT' || reason === 'ECONNRESET') return `${where} did not answer. A firewall on that computer may be blocking YourHarness (on Windows, allow it on Private networks), or the address is wrong.${macHint}`;
  return `Could not reach ${where} (${reason}).${macHint}`;
}

const localAddresses = () => new Set(['127.0.0.1', '::1', ...Object.values(os.networkInterfaces()).flat().filter(Boolean).map((i) => i.address)]);

// ---------------------------------------------------------------- joining or starting a pool
async function showPool(url, token) {
  poolUrl = url;
  await win.loadURL(`${url}/?native=1${token ? `#join=${token}` : ''}`);
  setStatus();
}

async function startOwn() {
  killAll(); // leave any pool this app was sharing with
  const env = { HARNESS_BEACON_PORT: String(BEACON_PORT) };
  let child = run(['serve', '--host', '0.0.0.0', '--port', String(OWN_PORT), '--no-qr'], env);
  let m;
  try {
    m = await waitFor(child, /Listening on port (\d+)/);
  } catch (e) {
    child.kill();
    child = run(['serve', '--host', '0.0.0.0', '--port', '0', '--no-qr'], env); // the usual port is taken
    m = await waitFor(child, /Listening on port (\d+)/);
  }
  const port = m[1];
  ownPort = Number(port);
  const tokenLine = child.tail.join('\n').match(/#join=([A-Za-z0-9_-]+)/);
  const token = tokenLine ? tokenLine[1] : '';
  writeCfg({ mode: 'own' }); // the pool's own agent shares this computer and hosts its terminal, on Mac and Windows
  await showPool(`http://127.0.0.1:${port}`, token);
}

function parseAddress(text) {
  const m = /^(?:https?:\/\/)?([^\/:\s]+)(?::(\d{1,5}))?\/?$/.exec(String(text || '').trim());
  return m ? { host: m[1], port: Number(m[2] || 8420) } : null;
}

async function joinRemote(info) {
  const addr = info.address ? parseAddress(info.address) : info;
  if (!addr || !addr.host) return { error: 'That address does not look right. Use something like 192.168.1.51:8420.' };
  const { host, port, code } = { ...addr, code: info.code };
  const url = `http://${host}:${port}`;
  const probe = await check(url);
  if (!probe.ok) return { error: explain(host, port, probe.reason) };
  // --workspace: this computer offers its own terminal to the pool too (Ubuntu via WSL or PowerShell on Windows)
  const agent = run(['agent', '--server', url, '--join', code, '--workspace']);
  try {
    await waitFor(agent, /Joined the pool/, 25000);
  } catch (e) {
    agent.kill();
    return { error: e.message.includes('code') || e.message.includes('join') ? e.message : `Could not join. ${e.message}` };
  }
  killAll(agent); // stop sharing with the old pool: its coordinator, if this app ran one, and its agent
  writeCfg({ mode: 'remote', server: url });
  await showPool(url, code);
  return { ok: true };
}

async function rejoinSaved(url) {
  const agent = run(['agent']); // reuses the sign-in saved on first join
  try { await waitFor(agent, /Joined the pool/, 20000); } catch { agent.kill(); return false; }
  await showPool(url);
  return true;
}

async function boot() {
  const cfg = readCfg();
  if (cfg.mode === 'remote' && cfg.server && await reachable(cfg.server) && await rejoinSaved(cfg.server)) return;
  if (cfg.mode === 'own') { try { await startOwn(); return; } catch (e) { log('own failed', e.message); } }
  const found = await discover();
  if (!found.length) { try { await startOwn(); return; } catch (e) { log('own failed', e.message); } }
  await win.loadFile(path.join(__dirname, 'pair.html'));
}

// ---------------------------------------------------------------- window and tray
function showPairPage() {
  showWindow();
  win.loadFile(path.join(__dirname, 'pair.html'));
}

function buildMenu() {
  const join = { label: 'Join another pool…', accelerator: 'CmdOrCtrl+J', click: showPairPage };
  const template = [
    ...(MAC ? [{ role: 'appMenu' }] : []),
    { label: 'File', submenu: [join, { type: 'separator' }, MAC ? { role: 'close' } : { role: 'quit' }] },
    { role: 'editMenu' },
    { role: 'viewMenu' },
    { role: 'windowMenu' },
  ];
  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

function setStatus() {
  if (!tray) return;
  tray.setToolTip(poolUrl ? `YourHarness: sharing with ${poolUrl}` : 'YourHarness');
  tray.setContextMenu(Menu.buildFromTemplate([
    { label: 'Open YourHarness', click: showWindow },
    { label: 'Join another pool…', click: showPairPage },
    { label: poolUrl ? `Pool: ${poolUrl}` : 'Not connected', enabled: false },
    { label: 'Copy pool address', enabled: !!poolUrl, click: () => clipboard.writeText(poolUrl) },
    { type: 'separator' },
    { label: 'Quit (stops sharing this computer)', click: () => { quitting = true; app.quit(); } },
  ]));
}

function showWindow() {
  if (!win) return;
  if (win.isMinimized()) win.restore();
  win.show();
  win.focus();
}

function sameOrigin(a, b) {
  try { return new URL(a).origin === new URL(b).origin; } catch { return false; }
}

function createWindow() {
  win = new BrowserWindow({
    width: 1280, height: 860, minWidth: 360, minHeight: 500, show: false, title: 'YourHarness',
    backgroundColor: '#11161D',
    webPreferences: { preload: path.join(__dirname, 'preload.js'), contextIsolation: true, sandbox: true, nodeIntegration: false },
  });
  win.once('ready-to-show', () => win.show());
  win.on('close', (e) => { if (!quitting) { e.preventDefault(); win.hide(); } }); // keep sharing in the background
  win.webContents.setWindowOpenHandler(({ url }) => { shell.openExternal(url); return { action: 'deny' }; });
  win.webContents.on('will-navigate', (e, url) => {
    if (url.startsWith('file:') || (poolUrl && sameOrigin(url, poolUrl))) return;
    e.preventDefault();
    shell.openExternal(url);
  });
  const toggleFullScreen = () => win.setFullScreen(!win.isFullScreen());
  win.webContents.on('before-input-event', (e, input) => { if (input.type === 'keyDown' && input.key === 'F11') { toggleFullScreen(); e.preventDefault(); } });
}

app.on('second-instance', showWindow);
app.on('activate', showWindow);
app.on('before-quit', () => { quitting = true; killAll(); });
app.on('window-all-closed', () => { /* stay in the tray */ });

// Answer only the app's own pages, never a pool page that reached the bridge somehow.
const fromApp = (e) => { try { return new URL(e.senderFrame.url).protocol === 'file:'; } catch { return false; } };
const refused = { error: 'Only the YourHarness app itself can do that.' };

ipcMain.handle('servers', (e) => (fromApp(e) ? discover() : []));
ipcMain.handle('pair', (e, info) => {
  if (!fromApp(e)) return refused;
  if (!info || typeof info !== 'object' || !/^\d{4}$/.test(String(info.code || ''))) return { error: 'The join code is 4 digits.' };
  return joinRemote({ address: String(info.address || ''), code: String(info.code) });
});
ipcMain.handle('start-own', async (e) => {
  if (!fromApp(e)) return refused;
  try { await startOwn(); return { ok: true }; } catch (err) { return { error: err.message }; }
});

app.whenReady().then(async () => {
  createWindow();
  buildMenu();
  const icon = nativeImage.createFromPath(path.join(__dirname, 'tray.png')).resize({ width: 18, height: 18 });
  tray = new Tray(icon);
  tray.on('click', showWindow);
  setStatus();
  try { await boot(); } catch (e) { log('boot failed', e.stack || e.message); win.loadFile(path.join(__dirname, 'pair.html')); }
});
