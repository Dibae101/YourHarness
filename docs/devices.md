# Setting up each device

The README's Quick start brings up the coordinator and joins computers and browsers. This page adds what each kind of device needs for batch jobs, test suites, containers and agent tasks, and how a friend lends a computer. The rules for what each device may run are in [security.md](security.md).

## The coordinator's computer

`harness serve` (or the desktop app, which starts one when it finds no pool) runs the coordinator and joins the same computer to the pool, so it computes and hosts a workspace too. It keeps its data in `~/.yourharness/coordinator` (sign-ins, `engine/engine.db`, stored datasets and results). Back that folder up if the results matter.

Final checks of agent runs (the tests on the combined branch) run on this computer, so it needs Git and whatever the repository's checks need.

## Your Mac or Linux computer

```bash
curl -fsSL http://<coordinator>:8420/install.sh | bash -s -- --join '<join link or code>'
```

Or, from a copy of the repository: `python3 -m venv .venv && .venv/bin/pip install -e . && .venv/bin/harness agent --join '<join link>'`. Add `--workspace` on the computer that should host the shared terminal.

Optional, per kind of job:

| For | Install |
| --- | --- |
| Jobs with their own Python environment (`managed-python`) | Nothing: uv is downloaded (checksum checked) the first time you approve a setup with downloads. Or install it yourself (`brew install uv`, `pipx install uv`) |
| Jobs in a container (`isolated-oci`) | Docker Desktop, Docker Engine or Podman, running. The agent finds it at start (or names it with `HARNESS_OCI_ENGINE=/path/to/podman`; `HARNESS_OCI=0` turns containers off). The Batch app's Computers tab says "containers: docker 27.x" when it's found |
| Test suites (`harness test`) with the default runtime | pytest and the project's packages in the Python the agent runs with |
| ML evaluation with TorchScript models | torch in the job's Python (or a managed environment with it) |
| Agent tasks | Git, curl, and the AI agent CLIs you want to use, signed in (see [agent-tasks.md](agent-tasks.md)) |

## Your Windows PC

`irm http://<coordinator>:8420/install.ps1 | iex` in PowerShell installs Ubuntu under WSL if needed and the agent inside it. Batch jobs, managed Python and test suites run inside Ubuntu. For containers, install Docker inside Ubuntu (or Docker Desktop with WSL integration) so `docker` works in the Ubuntu shell.

## The desktop app

The Mac and Windows apps join a pool with a code, share the computer from the menu bar or tray, and open the workspace in a window. While the computer works for the pool the app keeps it from suspending; the tray menu chooses when: only on mains power (default), on battery too, or never. The screen may still turn off. Jobs with their own Python environment or a container need the engine's worker as source files, to put in the environment or mount into the container. If the Batch app's Computers tab doesn't list `managed-python` or `isolated-oci` for a computer running the app, install the agent there from the installer or the repository.

## Phones, tablets and other browsers

Open the coordinator's address and enter the code. A tab shares its processor while the page is open and visible: it takes pieces of the older compute jobs and partitions of batch totals and group-bys. It hands its partitions back as soon as the page is hidden or the screen locks, so keep it in front while you want it to work. Below 20% battery and not charging it takes no batch partitions. The first time, it runs a short benchmark; the Batch app's Computers tab shows the result and why a tab isn't taking work.

A phone or tablet takes small tasks only: the pool uses one of its threads, whatever its own slider says, until you give it more on Home. It's mostly for watching what the pool is doing.

Over HTTPS (see [security.md](security.md)) the page can also keep the screen on while it works.

## Pausing a device, or limiting its threads

Home lists every connected device. As the pool's owner you get two controls on each:

- **The switch** pauses the device. It takes no new work of any kind: compute jobs, batch partitions, test shards, agent tasks or pool commands. What it's running finishes. Turn it back on to resume.
- **− and +** set how many of its threads the pool may use, up to what the device offers. On a computer, going back up to the full count removes the limit.

The coordinator keeps both with the device's sign-in, so they hold when the device reconnects or the coordinator restarts, and it enforces them, so a device can't take more than you allow. The device's own sharing switch and thread slider still count: the pool uses the smaller of what the device offers and what you allow. The device says so too: a browser under **This device**, an agent in its terminal.

From a script: `POST /api/devices/{id}/limits` with `{"paused": true}`, `{"threads": 2}`, or `{"threads": null}` for no limit (owner only). Each change goes in the audit trail.

## A friend's computer

1. In the Agents app, Computers tab, **Invite a friend's computer** → **Make a code**. The code works once, for 24 hours.
2. Your friend runs the one-liner the app shows for their Mac, Linux computer or Windows PC. It installs the agent and joins with the code. The Mac/Linux line includes `--tasks` (agent tasks, edit-only); remove it if they only lend compute.
3. By default their computer takes only batch partitions of the built-in operators, on datasets you marked shareable, for jobs that allow friends. It never runs your code, opens a terminal, or lets you see its files.
4. They can also allow agent tasks with `harness agent --tasks` (edit-only AI agents), or `--allow-commands` to also run your setup and check commands.
5. They can see what their computer did: `harness agent --activity`. To stop: quit the agent. To leave for good: `harness agent --cleanup` deletes what pools left on the computer and its sign-in.

When you remove their computer, it deletes what it kept for the pool on its own, and you can still see what it did under **Activity**.

## Checking a device

```bash
harness devices                # what each device reports: cores, memory, disk, GPUs
harness job plan job.json      # which devices would run a job, and why the others wouldn't
```

The Batch app's Computers tab shows what each computer can run: operators, formats, runtimes, prepared environments, containers, GPUs, and for browser tabs their measured speed.
