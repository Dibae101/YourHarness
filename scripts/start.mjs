// Launch the current coordinator from a source checkout on Windows, macOS or Linux.
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn, spawnSync } from 'node:child_process';

const root = fileURLToPath(new URL('../', import.meta.url));
const windows = process.platform === 'win32';
const python = join(root, '.venv', windows ? 'Scripts/python.exe' : 'bin/python');
function run(command, args) {
  const r = spawnSync(command, args, { cwd: root, stdio: 'inherit' });
  if (r.error) console.error(r.error.message);
  if (r.status !== 0) process.exit(r.status || 1);
}
if (!existsSync(python)) {
  const candidates = windows ? [['py', '-3'], ['python']] : [['python3'], ['python']];
  const candidate = candidates.find(
    ([cmd, ...args]) =>
      spawnSync(cmd, [...args, '-c', 'import sys; sys.exit(sys.version_info < (3,10))']).status === 0,
  );
  if (!candidate) {
    console.error('Install Python 3.10 or newer, then run npm start again.');
    process.exit(1);
  }
  run(candidate[0], [...candidate.slice(1), '-m', 'venv', '.venv']);
}
if (spawnSync(python, ['-c', 'import aiohttp, numpy, PIL, imageio_ffmpeg, qrcode']).status !== 0) {
  run(python, ['-m', 'pip', '--disable-pip-version-check', 'install', '-e', '.[qr]']);
}
const app = spawn(python, ['-m', 'yourharness', 'serve', ...process.argv.slice(2)], {
  cwd: root,
  stdio: 'inherit',
});
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => app.kill(signal));
app.once('error', (error) => {
  console.error(error.message);
  process.exitCode = 1;
});
app.once('exit', (code, signal) => {
  process.exitCode = code ?? (signal === 'SIGINT' ? 130 : 1);
});
