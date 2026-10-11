# YourHarness

Pool the devices you already own into one workspace for AI work. Open a Linux terminal from any of them, and the jobs you run there borrow processors, memory and storage from the rest.

Plan and roadmap: https://dibae101.github.io/YourHarness/

## Install the apps

Download from the [Releases page](https://github.com/Dibae101/YourHarness/releases). Nothing else to install, no Python needed.

| Device | Get | Notes |
| --- | --- | --- |
| **Mac** (Apple silicon) | `YourHarness-…-mac-arm64.dmg` | Drag to Applications. The app isn't signed with a paid Apple certificate, so the first time: right-click the app, choose **Open**, then **Open** again. If macOS calls it **damaged** (builds up to v0.7.0 did), it isn't: run the two commands under [If the Mac app says it's damaged](#if-the-mac-app-says-its-damaged). |
| **Windows** | `YourHarness-…-win-x64.exe` | Run the installer. SmartScreen may warn because the app isn't code-signed: choose **More info**, then **Run anyway**. |
| **iPhone / iPad** | The pool's address in Safari | Open the address, tap Share, then **Add to Home Screen**. It then opens full screen like an app. Enter the join code once. |

#### If the Mac app says it's damaged

macOS shows "YourHarness is damaged and can't be opened" when a signature is **broken**, not when a download is corrupt — so redownloading doesn't help, and `xattr` alone doesn't either. Electron's binaries arrive ad-hoc signed; packaging renames the bundle and adds the Python engine, which invalidated that signature in builds up to v0.7.0. On Apple silicon the kernel refuses to run an executable whose signature doesn't check out.

Later builds are ad-hoc signed after packaging, so they only need the usual right-click → **Open**. To repair a disk image you already have, sign it yourself — it takes a second and needs no Apple account:

```bash
xattr -cr /Applications/YourHarness.app
codesign --force --deep --sign - /Applications/YourHarness.app
```

Then open it normally. If `codesign` isn't found, run `xcode-select --install` first.

**What the app does when you launch it:** it looks for a pool on your network. If it finds one it asks for the 4-digit code once, then reconnects by itself on every launch. If it finds none, it starts a pool on that computer. The computer shares its processors in the background (menu bar on Mac, tray on Windows); closing the window keeps sharing, and Quit stops it. The workspace terminal opens in the window. On the Mac the terminal is the Mac's shell; on Windows, add Ubuntu with the PowerShell command below for a Linux terminal.

### YourHarness OS

The Terminal app opens **YourHarness OS**: a shell that belongs to the pool, not to any computer.

- **Every command runs on the least-busy computer** and the line after it says where: `↳ ran on DESKTOP-KAMSS80 (Ubuntu on Windows (WSL)) · 1.8 s · exit 0`.
- **Its disk is `/pool`.** Before a command runs, the files that changed elsewhere are fetched to that computer; afterwards, what the command created, changed or deleted is saved back. A file written on the Mac is there when the next command lands on the Windows PC. The same disk is the first thing in the Files app.
- **Linux first.** Computers running Linux (Ubuntu, or Ubuntu under WSL on Windows) run the commands. A Mac is used only when no Linux computer is connected, or when you ask for it.

| Command | What it does |
| --- | --- |
| `on <computer> <command>` | Run on a specific computer (`linux`, `mac` or part of a name also work; quote names with spaces) |
| `par <command with {}> ::: a b c` | Run once per item at the same time, spread over the computers |
| `runners`, `ps` | Which computers can run commands and how busy they are; what ran where |
| `cd`, `pwd`, `export NAME=value`, `clear`, `help`, `exit` | Shell basics. Tab completes names in `/pool`, arrows scroll history, Ctrl-C stops the running command |

**What it is not.** One command still runs on one computer, so a single program doesn't get faster; `par`, `pool.map()` and Jobs are how work spreads over several. A computer's memory can't be added to another's (Wi-Fi is about 30,000 times slower than RAM). Commands get no keyboard input, so editors and `htop` need a **direct shell** (the other tabs in Terminal, one per computer). Each computer runs one command at a time; others wait. If two computers change the same file at once, the later save wins. Files are copied to each computer that runs a command, so a very large `/pool` makes the first command on a new computer slow.

### The desktop

The app (and the page in any browser) is a small desktop: a status bar on top, a dock at the bottom, and windows you can move, resize, minimize and maximize. On phones each app opens full screen.

| App | What it does |
| --- | --- |
| **System Monitor** | One card per computer or phone with what it shares, live CPU and memory use, and a two-minute CPU graph. Below: **Programs running in YourHarness OS** with the computer each is on. **Test the pool** runs a short job and shows how many pieces each device computed. |
| **Terminal** | **YourHarness OS** (default) plus a direct shell on each computer. |
| **Files** | Opens on `/pool`. Make folders, upload, download, rename and delete; each file shows which computers hold it. Also browse the home folder of every computer running the app, send a file from one computer to another, and the shared folder. |
| **Jobs** | Run a job across every sharing device and watch who computes which piece. |
| **Devices** | The join code and address, this device's sharing settings, and every connection with a Remove button. |

The status bar shows the connection and how fast the pool answers (for example `Pool 3 ms`), average CPU use across the pool's computers, which computer you're on, the join code and the time. Wi-Fi itself can't be controlled from a web page.

Computers that run the app let the pool browse their home folder and run commands, so anyone in the pool can do both. Share the join code only with people you trust.

Releases are built by GitHub Actions: push a tag such as `v0.3.0` and the `.dmg` and `.exe` appear on the Releases page. Run `python desktop/make_icons.py` to regenerate icons.

## What's here

| Part | What it does | Runs on |
| --- | --- | --- |
| **Coordinator** (`harness serve`) | Signs devices in, schedules work, hosts the shared folder, relays the workspace terminal | One machine on your network (today: the Mac) |
| **Agent** (`harness agent`) | Shares a computer's processors natively in Python, reports real memory, disk and GPUs, and can host the workspace | Mac, Linux, or Ubuntu under WSL on Windows |
| **Web page** | Dashboard for everyone. Phones and tablets share compute from it in JavaScript | Any browser |
| **Workspace** | A Linux terminal you open from any device's browser. With tmux, every device attaches to the same session | The agent started with `--workspace` (here: the Windows PC) |
| **`harness` and `pool.map()`** | Run jobs on the pool from the workspace shell or from Python | Inside the workspace, or any agent computer |

The first prototype (Node, `server.js` and `public/`) is still in the repo and still runs with `npm start`. It will be removed once the Python version replaces it.

## Quick start

### 1. Start the coordinator (Mac)

Stop the Node prototype first if it's running, since both use port 8420.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[qr]'
.venv/bin/harness serve
```

It prints a 4-digit join code, a QR code, and the command for adding computers:

```
Phones, tablets and browsers: open this address and enter the join code
  http://192.168.1.51:8420

  JOIN CODE: 4821

Computers (Mac, Linux, or Ubuntu in WSL on Windows): run this in a terminal
  curl -fsSL http://192.168.1.51:8420/install.sh | bash -s -- --join 4821 --workspace
```

The code stays the same across restarts. After 20 wrong codes from anywhere it changes and joining by code turns off, so the code can't be guessed; the long join link (also printed) keeps working. Press **New code** under Add a device (or restart the coordinator) to turn codes back on. `harness serve --new-join-link` replaces it; devices that already joined stay signed in. If macOS asks whether Python may accept incoming connections, allow it.

The coordinator's own computer joins the pool and hosts a workspace by itself, so you have a terminal as soon as it starts. `harness serve --no-local-agent` turns that off.

### 2. Join phones, tablets and browsers

Scan the QR code, or open the coordinator's address and type the code. The device signs in once and remembers it. Keep the page open with the screen on; a phone whose page is paused stops checking in, and its pieces move to other devices within about 20 seconds.

### 3. Add the Windows PC with its Ubuntu workspace

In PowerShell on the Windows PC (as administrator the first time, so it can install Ubuntu):

```powershell
irm http://<coordinator address>:8420/install.ps1 | iex
```

It installs Ubuntu under WSL if needed (restart and run it again if Windows asks), asks for the 4-digit join code, then installs the agent inside Ubuntu. Keep that PowerShell window open while the PC is in the pool. Once it's connected, the Ubuntu terminal is used in place of the Mac's. The manual steps below do the same thing.

#### Manual steps

In PowerShell as administrator, install Ubuntu under WSL, then restart if asked:

```powershell
wsl --install -d Ubuntu
```

Open Ubuntu from the Start menu and run (use the code the coordinator printed):

```bash
sudo apt update && sudo apt install -y python3-venv tmux curl
curl -fsSL http://192.168.1.51:8420/install.sh | bash -s -- --join '<join link>' --workspace
```

Use the exact command the coordinator printed, with your code. The installer puts everything in `~/.yourharness` and starts the agent. Next time, start it with `~/.yourharness/bin/harness agent`.

Leave out `--workspace` on other computers. They still share compute, memory and disk.

### 4. Work in the workspace

Press **Open terminal** on any device. Inside the shell:

```bash
harness devices                      # every device, with real memory, disk and GPUs
harness run primes --below 1e10      # a built-in job across the pool
harness run python job.py --pieces 64
harness files put dataset.csv        # the shared folder
```

From Python:

```python
from yourharness import pool

def score(n):
    import math              # import inside the function: it runs on other devices
    return sum(math.sqrt(i) for i in range(n))

results = pool.map(score, [5_000_000] * 64)   # spread over every agent
```

Functions are sent as source code, so define them with `def`. Arguments and results must be JSON: numbers, text, lists, dicts.

## Pooled storage

Add files in the **Pooled storage** panel, or from the workspace:

```bash
harness pool put dataset.csv      # cut into chunks, two copies on different computers
harness pool ls                   # copies per file, free space, which files are offline
harness pool get dataset.csv .
harness pool rm dataset.csv
```

Only computers running an agent hold chunks; phones and tablets add and read files but store nothing. With one agent there is one copy. If a computer holding a copy disconnects, the file still downloads from the other copy and the missing copies are rebuilt on remaining computers within about 10 seconds. If every computer holding a chunk is offline, the file shows as offline until one returns. Chunks are checked against their SHA-256 on every read. Files pass through the coordinator, so its network is the speed limit.

## What can be pooled

Wi-Fi is about 30,000 times slower than memory inside a device, so the pool never pretends to be one big computer. It splits work so each device computes on its own memory and only inputs and results cross the network.

| Resource | Today | Next |
| --- | --- | --- |
| Processors | Jobs split into pieces; every device pulls a piece when it has a free thread | |
| Storage | **Pooled storage:** files are cut into 4 MB chunks, two copies each on different computers, repaired when one leaves. A separate shared folder on the coordinator is used by the file-search job | A `/pool` folder mount (needs FUSE) |
| Memory | Each device works in its own memory | Model layers split across devices by free memory |
| GPUs | Reported by agents | CUDA and Metal through agents, WebGPU in browsers over HTTPS |

## How it works

- **Pull scheduling.** A device gets a new piece whenever one of its threads is free, so a fast PC naturally does more than a phone.
- **Check-ins.** Every device sends a small message every 4 seconds. One that goes quiet for 20 seconds is marked as not responding and its pieces go to other devices. This catches frozen phone pages, which a WebSocket ping doesn't, because browsers answer pings from their network layer. A piece that runs longer than `CHUNK_TIMEOUT_S` is also handed to someone else.
- **Runtimes.** Browsers run JavaScript, agents run Python. Built-in jobs run on both and their results mix in one run. Your own code goes only to devices that can run its language.
- **Job settings are sent once per device**, not with every piece.
- **Agents run pieces in separate worker processes**, so they run in parallel and a cancelled or stuck piece is stopped by ending its process. `print()` in your code shows in the agent's terminal.
- **The workspace terminal** is a PTY on the hosting agent. The coordinator only relays bytes between it and the browser that opened it.

## Security

- Devices sign in with the 4-digit join code or a 144-bit join link and then use their own device token. Browsers keep it in an HttpOnly, SameSite=Strict cookie, so no secret stays in the page or in URLs. Agents store it in `~/.yourharness/agent.json` (mode 600).
- After 10 failed sign-ins from one address in 10 minutes, that address is refused for the rest of the window. Devices that are already signed in are never locked out.
- After 20 wrong join codes from all addresses together, joining by code turns off until an owner makes a new code.
- A device that joined with a lend link only computes run pieces. It never hosts terminals, runs pool shell commands, holds /pool files or serves the Files app, whatever its agent claims, and it isn't sent file names or command lines.
- Requests and WebSocket connections from other websites are refused.
- Shared files are always served as downloads, so an HTML file in the shared folder can't run as a page on the pool's address.
- Remove a lost device from the Devices list. Its token stops working at once.
- **Anyone in the pool can open the workspace terminal and run code on every sharing device.** Share the join link only with people you'd hand your computers to.

For HTTPS, which also lets phones keep the screen on and use WebGPU, put the coordinator behind something that terminates TLS, such as `tailscale serve --bg 8420`, and start it with `--public-url https://<name>.ts.net`. Behind a proxy, every device appears to come from the proxy's address unless you set `HARNESS_TRUST_PROXY=1`, which uses the last X-Forwarded-For entry (the one the proxy added).

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `--port` / `PORT` | `8420` | Port to listen on |
| `--host` / `HOST` | `0.0.0.0` | Address to bind |
| `--data-dir` / `HARNESS_DATA_DIR` | `~/.yourharness/coordinator` | Join link and device sign-ins |
| `--shared-dir` / `SHARED_DIR` | `~/.yourharness/shared` | The shared folder |
| `--public-url` | this machine's LAN address | Address printed in the join link |
| `MAX_UPLOAD_MB` | `4096` | Largest file you can add |
| `HARNESS_TRUST_PROXY` | unset | Set to `1` behind a proxy so each device keeps its own address (X-Forwarded-For) |
| `HARNESS_STALE_S` | `20` | Seconds without a check-in before a device's pieces move |
| `CHUNK_TIMEOUT_S` | `300` | Seconds before a running piece is given to another device |

Agent: `harness agent [--join LINK] [--name NAME] [--threads N] [--workspace | --no-workspace] [--no-share]`. Settings are remembered in `~/.yourharness/agent.json`.

## Development

```bash
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest
```

The tests start a real coordinator and real agents as subprocesses and connect fake browsers. They cover sign-in and lockout, cross-site requests, removing devices, jobs split between an agent and a browser, routing code by language, `pool.map`, shared files and search, rejecting malformed results, frozen devices losing their pieces, the workspace terminal, and the installer bundle. They pass on Python 3.12 and 3.14.

```
yourharness/
  cli.py              the harness command
  pool.py, client.py  pool.map() and the API client (standard library only)
  kernels.py          Python compute for every job type
  sysinfo.py          memory, disk and GPU detection
  coordinator/        app.py (HTTP, WebSocket), scheduler.py, jobkinds.py, terminals.py, files.py, auth.py, state.py, install.py
  agent/              agent.py, slots.py (worker processes), slot.py, terminal.py (PTY)
  web/                the page: index.html, app.js, jobs.js, worker.js, style.css, vendor/xterm
tests/                end-to-end tests
server.js, public/    the Node prototype (npm start, npm test)
```
