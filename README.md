# YourHarness

Pool the devices you already own into one workspace for AI work. Open a Linux terminal from any of them, and the jobs you run there borrow processors, memory and storage from the rest.

Plan and roadmap: https://dibae101.github.io/YourHarness/

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

The code stays the same across restarts. After 20 wrong guesses in a row it changes by itself, and the long join link (also printed) keeps working. `harness serve --new-join-link` replaces it; devices that already joined stay signed in. If macOS asks whether Python may accept incoming connections, allow it.

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

## What can be pooled

Wi-Fi is about 30,000 times slower than memory inside a device, so the pool never pretends to be one big computer. It splits work so each device computes on its own memory and only inputs and results cross the network.

| Resource | Today | Next |
| --- | --- | --- |
| Processors | Jobs split into pieces; every device pulls a piece when it has a free thread | |
| Storage | One shared folder on the coordinator, readable by byte range | Chunks stored on several devices, mounted as `/pool` |
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
- After 10 failed sign-ins from one address in 10 minutes, that address is refused for the rest of the window.
- Requests and WebSocket connections from other websites are refused.
- Shared files are always served as downloads, so an HTML file in the shared folder can't run as a page on the pool's address.
- Remove a lost device from the Devices list. Its token stops working at once.
- **Anyone in the pool can open the workspace terminal and run code on every sharing device.** Share the join link only with people you'd hand your computers to.

For HTTPS, which also lets phones keep the screen on and use WebGPU, put the coordinator behind something that terminates TLS, such as `tailscale serve --bg 8420`, and start it with `--public-url https://<name>.ts.net`. Behind a proxy, every device appears to come from the proxy's address, so the sign-in limit then applies to all of them together.

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `--port` / `PORT` | `8420` | Port to listen on |
| `--host` / `HOST` | `0.0.0.0` | Address to bind |
| `--data-dir` / `HARNESS_DATA_DIR` | `~/.yourharness/coordinator` | Join link and device sign-ins |
| `--shared-dir` / `SHARED_DIR` | `~/.yourharness/shared` | The shared folder |
| `--public-url` | this machine's LAN address | Address printed in the join link |
| `MAX_UPLOAD_MB` | `4096` | Largest file you can add |
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
