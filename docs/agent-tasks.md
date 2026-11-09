# Agent tasks

Give each computer in the pool a task, pick what does it (Claude Code, Codex CLI, Kiro CLI, or a plain command with no AI) and the model, and say which GitHub repository the results go to. Every task works in its own copy of the repository on its computer. The coordinator checks each result, combines the accepted ones on one branch, pushes that branch, and opens a draft pull request. Nothing is ever pushed to `main`, and nothing is merged for you.

Everything is in the **Agents** app (dock or app tiles) of the YourHarness page, on any device, including the iPhone.

- **Runs:** every run with its tasks, live status, log, changes, cost, and the branch and pull request.
- **New run:** the repository, the branch to start from, the branch for the results, and the tasks. Each task gets a computer, an agent, a model, instructions, checks, and the files it may change.
- **Computers:** which computers can run tasks and which agents each has; one-time codes for friends' computers.
- **GitHub:** connect a token. It stays on the coordinator.

## Try it this morning

The installed apps (v0.7.x) don't have this yet. Run it from the branch instead, and start with tasks that use no AI.

### 1. Start the pool on the Mac

Quit the YourHarness app on the Mac and on the PC first (menu bar or tray → Quit), and close any old YourHarness window in PowerShell on the PC. The Mac app uses the same port, and an old app on the PC would show up as a second computer without agent tasks.

```bash
cd ~/Developer/YourHarness
git fetch origin && git switch feat/agent-tasks && git pull
python3 -m venv .venv && .venv/bin/pip install -q aiohttp numpy
.venv/bin/python -m yourharness serve
```

It prints the pool address, the 4-digit join code and links. The Mac joins its own pool as a computer straight away, and takes agent tasks. If macOS asks whether Python may accept incoming network connections, choose **Allow**, or the PC can't join.

### 2. Add the Windows PC (through Ubuntu in WSL)

In PowerShell on the PC, using the address the Mac printed:

```powershell
irm http://<mac-address>:8420/install.ps1 | iex
```

Type the 4-digit code when asked. The agent is installed inside Ubuntu with the code from the Mac, so it has agent tasks too. Keep the window open.

### 3. Open the page

Open `http://<mac-address>:8420` in a browser on any device, type the code, then open **Agents → Computers**. Both computers should show **Ready**, with the agents each one has (Command always, plus Claude Code, Codex CLI and Kiro CLI if they are installed there).

### 4. A first run with no AI

1. On GitHub, make a new repository with a README (an empty repository has nothing to start from), or use one you don't mind getting a new branch.
2. **Agents → GitHub:** paste a fine-grained token (see below) and press **Connect**.
3. **New run:** pick the repository. Add two tasks:
   - Task 1: computer **the Mac**, done by **Command (no AI)**, command `echo "from the Mac" > mac.txt`.
   - Task 2: computer **the PC**, done by **Command (no AI)**, command `uname -a > pc.txt`.
4. **Start the run.** In **Runs**, both tasks go from Waiting to Running to Done, and the run pushes a branch `yh/r…` and opens a draft pull request with both files.

### 5. Then with AI agents

Sign in to each agent CLI on the computer that will run it, once, in its own terminal: `claude auth` (or just `claude`), `codex login`, `kiro-cli login`. On the PC, do this inside Ubuntu. Then start a run with a real task, for example *“Add a CONTRIBUTING.md that explains how to run the project and its tests”*, with **Claude Code** on one computer and **Codex CLI** on the other.

If a computer doesn't list an agent you installed, restart its agent. It looks for agents when it starts, and again every 10 minutes when it's idle.

## The GitHub token

Make a **fine-grained personal access token**: GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens → Generate new token.

- **Repository access:** only the repositories YourHarness should push to.
- **Permissions → Repository:** **Contents: Read and write**, **Pull requests: Read and write**. Metadata stays read-only.
- Pick an expiry you're comfortable with. Revoke it on GitHub at any time.

The token is stored in `~/.yourharness/coordinator/secrets.json` on the coordinator's computer, readable only by its user. It is never sent to other computers, never returned by the API, never logged, and never put in a Git URL. The coordinator passes it to `git` through the environment for each fetch and push. Computers doing tasks never talk to GitHub at all: they download their starting point from the coordinator and upload their result to it.

You can also use a Git repository on the coordinator's computer instead of GitHub: in **New run**, choose *A Git repository on the coordinator's computer* and type its path. Results go to a new branch there.

## Friends' computers

**Computers → Invite a friend's computer → Make a code** gives a one-time code like `ABCD-EFGH-JKLM-NPQR`, valid for 24 hours. Their computer joins with it in one of these ways:

- **Mac or Linux, in a terminal:** `curl -fsSL http://<address>:8420/install.sh | bash -s -- --join ABCD-EFGH-JKLM-NPQR --tasks`. It needs Python 3.10 or newer, and git for agent tasks. Leave out `--tasks` to only lend compute.
- **Windows, in PowerShell:** `irm http://<address>:8420/install.ps1 | iex`. It sets up Ubuntu, asks for the code, then asks whether to allow agent tasks and, separately, commands.

There are two levels of permission on a friend's computer:

- `--tasks` lets you give it AI agents that **may only edit files**: no setup or check commands, no Command agent, and no “run any command” mode.
- `--tasks --allow-commands` also lets you run commands there: the Command agent, setup and check commands, and AI agents allowed to run commands.
- **The YourHarness app,** from its next release: File → Join another pool, then the address and the friend code. They tick **Allow agent tasks** only if they agree. The v0.7 apps don't take friend codes.

The Agents app shows these commands with the address and the code filled in.

To try this without a second person, act as the friend on the Mac itself, in a second terminal. Make a code first, then:

```bash
cd ~/Developer/YourHarness
YOURHARNESS_HOME="$HOME/yh-friend-test" .venv/bin/python -m yourharness agent --server http://127.0.0.1:8420 --join ABCD-EFGH-JKLM-NPQR --tasks --allow-commands --no-share --name "Friend test"
```

It shows up in **Computers** as a friend's computer. Tasks go to it only when you pick it by name.

A friend's computer never controls the pool. It can't start runs, open terminals, read files, or see the GitHub token, exactly like the lend link. It runs agent tasks only when its owner allowed them, and only tasks you give it by name: **Any of my computers** never picks a friend's computer. When you give it a task, the New run screen reminds you that the repository's code goes to that computer and the agent runs with your friend's account and AI subscription. The result is checked like any other before anything is pushed.

Their computer has to reach the pool's address. On another network, connect both through something like Tailscale and start the pool with `harness serve --public-url https://<name>.<tailnet>.ts.net`.

## How a task runs

On the computer it was given to:

1. It downloads the run's starting commit from the coordinator, once per repository, and keeps a cache.
2. It creates a Git worktree at that commit in the agent's folder: `~/.yourharness/tasks/ws/<task>-<attempt>` (`~/.yourharness/local-agent/tasks/…` on the coordinator's own computer, inside Ubuntu for a PC set up with `install.ps1`). When the Windows app runs tasks itself, the folder is `~/.yourharness-tasks` inside Ubuntu, never under `/mnt/c`.
3. It runs the setup commands, then the agent (or the command), then the checks. When checks fail, the agent gets another round with the failure output, up to the round limit.
4. It commits everything as one commit, makes a Git bundle, and uploads it.

The agent, setup and check commands run with an emptied environment plus a short allowlist (`HOME`, `PATH`, proxies, and the variables the CLIs use, such as `ANTHROPIC_API_KEY` if you set it). The task's token, which can only download the starting point and upload the result, is only given to those two steps.

“Only edit files” is the default. “Edit files and run any command” turns the CLIs' permission checks off, so the agent can run any command as that computer's user; it has to be picked for each task.

| Agent | Command used | “Only edit files” (default) | “Edit files and run any command” |
| --- | --- | --- | --- |
| Claude Code | `claude -p … --output-format stream-json --verbose --no-session-persistence` | `--permission-mode acceptEdits` | `--permission-mode bypassPermissions` |
| Codex CLI | `codex exec --json --ephemeral -C <worktree> -o <last message>` | `-s workspace-write` (Codex's own sandbox) | `--dangerously-bypass-approvals-and-sandbox` |
| Kiro CLI | `kiro-cli chat --no-interactive` | `--trust-tools=fs_read,fs_write` | `--trust-all-tools` |
| Command (no AI) | `bash -c '<your command>'` | | |

The **Model** field is passed as `--model` (or `-m` for Codex); leave it empty for the CLI's default. The **Most it may spend** limit is Claude Code's `--max-budget-usd`. Every task also has a time limit; when it runs out, everything the task started is stopped.

## What is checked before anything is pushed

The coordinator imports each result and checks the following, then lists anything it found on the task:

- The result starts from the commit the task was given.
- Every changed file matches **Files it may change**. An empty list means any file, except for “Other work” tasks, which may only write in their own `results/<task>/` folder.
- No file matches **Files it must not change**. CI workflows (`.github/workflows/**`) and `.gitmodules` are protected unless the task allows workflow changes.
- No submodules, no symbolic links pointing outside the repository, no file over 50 MB, at most 200 commits, and at most 5,000 changed files.
- Nothing that looks like a secret: private keys, GitHub tokens, AWS keys, `sk-…` API keys, Slack tokens, Google API keys.

A flagged result shows **Accept anyway**, so you can read its changes and push it on purpose. Accepted results are cherry-picked onto the run's branch in task order. A task that conflicts with an earlier one is left out and says which files conflict; **Retry** redoes it on top of what the others already did. **Push now** pushes whatever is accepted so far, and **Run again** copies a run into New run.

## If something doesn't work

- **A computer shows “Git isn't installed”** — install git there (inside Ubuntu on Windows: `sudo apt install -y git curl`).
- **A task waits with “… isn't installed on <computer>”** — install that CLI on that computer and restart its agent, or pick another computer.
- **A task fails right after starting the agent** — open its **Log**. Usually the CLI isn't signed in on that computer: run it once by hand there.
- **“GitHub refused the token”** — the token needs Contents and Pull requests set to Read and write, for that repository.
- **“The branch … already has other commits”** — pick another results branch, or leave it empty to get a new `yh/r…` branch.
- **The Mac's app and the pool from the branch both want port 8420** — quit the app, or start the pool with `--port 8430`.

Logs for each task are in `~/.yourharness/coordinator/agent-tasks/logs/` on the coordinator. Runs and tasks are kept in `agent-tasks/tasks.db` there and survive a restart: a computer that was working on a task carries on and reports when it reconnects.

## What hasn't been tried yet

- The three AI adapters were built from each CLI's `--help` and haven't been run against real models. The automated tests use plain commands only. Expect to adjust flags or output parsing after the first real run; the log shows the agent's raw output when the parser doesn't recognize it.
- On Windows, the YourHarness **app** itself (not Ubuntu set up with `install.ps1`) runs tasks inside WSL through `wsl.exe`. That path hasn't been tested on a PC yet, so start with the `install.ps1` route above.
- Friends' computers have no sandbox beyond what each CLI provides. That's why your friend has to allow tasks, and why you give them tasks by name.
