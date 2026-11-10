# Security: who may run what, and what protects what

YourHarness lets computers you own work together, and lets friends lend theirs. The rules below decide what each device may receive. They are enforced by the coordinator before anything is offered, and again by the agent on the computer itself, so a coordinator that skipped a rule still couldn't run your code on a friend's computer.

**Anyone who joined with the owner link or the 4-digit code controls the pool**: they can open terminals and run code on every computer that hosts the workspace. Give that link only to people you'd hand your computers to. Friends get a friend code or the lend link instead.

## Kinds of device

| Tier | Who | Batch jobs | Agent tasks | Terminal, files, pool shell |
| --- | --- | --- | --- | --- |
| Your computers | Joined with the owner link or code | Everything: built-in operators and your code (natively, in a managed environment, or in a container), any dataset | Yes | Yes, when started with `--workspace` |
| Friends' computers | Joined with a one-time friend code or the lend link | Only the engine's built-in operators (totals, group-bys, transforms, joins), only on datasets you marked shareable, only for jobs that allow friends (`security.allowedTrust: owned-and-friends`). Never your code, not even in a container | Only when the friend allows it (`--tasks`); edit-only AI agents unless the friend also allows commands (`--allow-commands`) | Never |
| Browser tabs (yours or a friend's) | Opened the page and signed in | Built-in totals and group-bys, computed by the page's own operators; a friend's tab only for shareable data and jobs that allow friends | No | Your own tabs: yes. A friend's: never |

`GET /api/engine/policy` returns these rules as data. A device's claims about itself (its runtimes, its tools) are only believed for what its tier allows.

## What each runtime isolates

| Runtime | Isolation | Notes |
| --- | --- | --- |
| `builtin` | No code from the job runs | The engine's operators read the data and return a state |
| `native-python` | **None**: your code runs as the agent's user, with that user's files and network | For your own computers only |
| `managed-python` | Dependencies only: a uv environment with pinned packages | Same access as native. Not a sandbox |
| `isolated-oci` | A container: `--network none`, `--read-only`, `--cap-drop ALL`, `no-new-privileges`, not root, the job's CPU, memory and process limits, a 256 MB private `/tmp`, and only three folders mounted (the engine's worker code and the partition's input read-only, an output folder) | Checked once per image and limits on each computer, with the same flags, before any partition runs: no network, read-only, not root, limits applied, no container engine socket, no tokens in the environment. A failed check means the job doesn't run there; nothing falls back to running outside a container. The image is named by digest and never pulled while a job runs (`--pull never`). The computer fetches the input and uploads the outputs, so the code never sees the attempt's token |

A container shares the host's kernel. It's good isolation for your own code and for code you don't fully trust on your own computer, but it isn't treated as a sandbox for a friend's computer: escaping a container is a kernel or engine bug away, and nobody has reviewed this setup for that. On macOS and Windows, Docker runs containers in a Linux VM, which adds a layer. Rootless Podman or rootless Docker adds another: root inside the container is your own user outside.

## Data

Datasets are `owned-private` (default) or `shareable` (`harness dataset add --shareable`, or **Share** in the Batch app). Only shareable datasets reach friends' devices, and only for jobs that allow friends. A job that joins two datasets needs both shareable. Models and test projects never reach friends' devices, because only your own computers run the code that reads them.

Everything a job reads or writes is stored on the coordinator by SHA-256 and checked on every download. A model or project is pinned by its SHA-256 when the job is created. Zip archives are unpacked only when every path stays inside its folder and no entry is a link.

## Tokens

- A device signs in once and then uses a device token. Browsers keep it in an HttpOnly, SameSite=Strict cookie; agents in `~/.yourharness/agent.json` (mode 600). The coordinator stores only a SHA-256 of each secret.
- Each attempt at a partition gets a token of its own. It can read that partition's input and write that attempt's outputs, nothing else, and stops working when the attempt ends. A container never receives it.
- An agent task's token reads its own repository bundle and uploads its own result; the GitHub token never leaves the coordinator's computer.
- After 10 failed sign-ins from one address in 10 minutes, that address is refused for the rest of the window. After 20 wrong 4-digit codes from all addresses together, joining by code turns off.

## HTTPS

On your own network the pool uses plain HTTP. To reach it from elsewhere, or to give phones a secure context (needed for the screen Wake Lock and WebGPU), use HTTPS:

- **Tailscale (simplest):** `tailscale serve --bg 8420`, then `harness serve --public-url https://<machine>.<tailnet>.ts.net`. Every device on your tailnet gets a valid certificate and an encrypted connection, and nothing is exposed to the internet.
- **A certificate of your own:** `harness serve --tls-cert cert.pem --tls-key key.pem --public-url https://<a name it covers>:8420`. Use one your devices already trust (`tailscale cert`, Let's Encrypt), or one from a private authority such as mkcert. On computers that don't trust that authority, join with `harness agent --ca rootCA.pem --join '<link>'`: the agent, its workers, the CLI and agent-task downloads then check the coordinator's certificate against it (it's kept in `agent.json`; `HARNESS_CA_FILE` does the same). The coordinator's own computer uses a plain listener on 127.0.0.1 that only it can reach.

The install one-liners (`install.sh`, `install.ps1`) use the address you give them; with a private authority, install by copying the repository and running `harness agent --ca ... --join ...`.

When the coordinator can be reached from the internet, start it with `HARNESS_ONLINE=1`: the 4-digit code is then turned off and only links and friend codes work. TLS protects what travels between devices; it doesn't protect data from the computer chosen to process it. That's what the tiers above decide.

## Removing a device, and what it did

**Remove** in the Devices list (or `DELETE /api/devices/{id}`):

- its token stops working at once and it is disconnected;
- its partitions go to other computers at once, without waiting for the lease, and its agent tasks start again elsewhere;
- a **friend's computer** then deletes what it kept for the pool: partition files and caches, downloaded models and projects, prepared Python environments, container images it pulled, agent task folders, and its sign-in. An owner's computer keeps them (they may be your work in progress) and says how to delete them: `harness agent --cleanup`. The YourHarness program and anything outside those folders are never touched.

Every computer keeps its own record of what it did, in `~/.yourharness/activity.jsonl`: each partition (which pool, job and workload, which runtime, how many bytes it read) and each agent task (repository, agent). `harness agent --activity` prints it, also after the computer was removed. A friend can check what their computer was asked to do without taking your word for it.

The coordinator keeps the other side:

- **Activity** next to each device in the Devices list (`GET /api/devices/{id}/activity`): the jobs it worked on, which dataset and how it's classified, whether it ran your code or the built-in operators, the model, partitions tried and accepted, bytes read, and the agent tasks it ran with their repositories. It stays available after the device is removed. Any device can see its own (`/api/devices/me/activity`).
- **An audit trail** (`GET /api/audit`, owner only): devices joining (and whether as a friend), removals and who removed them, friend codes made, used and revoked, replaced links, and sharing changes from Home ("sharing changed": a device paused or resumed, or its threads limited, and by whom).

A friend code that expires or is revoked stops new joins with it; a computer that already joined has to be removed.

## What isn't protected

- Your own computers trust each other completely. Code you run natively can read everything the agent's user can, on every computer it runs on.
- A friend's computer sees the shareable data it processes, and the friend can read it from memory or disk while it works. Mark data shareable only when that's fine.
- Agent tasks on a friend's computer run AI agents there, in a Git worktree, as the friend's user; with `--allow-commands` they can run your setup and check commands too. That is the friend's choice to make.
- The coordinator's computer holds everything: the datasets, results, the GitHub token, and the sign-ins.
- The checks on agent-task results (file scope, protected files, things that look like secrets) are structural; they reduce mistakes, they don't prove a change is safe.
- Nobody has reviewed this for running a stranger's code. Don't.

## Reporting a problem

Open an issue on the repository without the details, and say how to reach you.
