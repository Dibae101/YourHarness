"""Who may run what: the trust rules applied before any partition is offered to a computer.

Computers come in tiers (the blueprint's trust tiers T1-T3):

  owned    your own computers: the built-in operators, and your code natively, in a
           managed Python environment, or in a container
  friend   a friend's computer that joined with a friend code: the built-in operators on
           datasets you marked shareable. Never a job's code, not even in a container: a
           container alone isn't a reviewed sandbox for someone else's machine
  browser  a browser tab, yours or a friend's: built-in totals and group-bys only

What each runtime isolates:

  builtin         the engine's own operators; no code from the job runs at all
  native-python   your code in the computer's own Python, as the agent's user: no isolation
  managed-python  the same, in an environment with pinned packages: dependencies only
  isolated-oci    your code in a container (Docker or Podman): no network, read-only system
                  files, not root, no added privileges, CPU, memory and process limits,
                  and only its own input and output folders mounted

There is no fallback: a job that needs a container never runs natively because no
computer had one ready, and a friend's computer never runs code because the job allowed
friends. The agent applies the same rules on its side (agent/enginehost.py), so a
coordinator that skipped them still couldn't run a job's code on a friend's computer.
"""

from __future__ import annotations

TIERS = {
    "owned": "your own computers",
    "friend": "friends' computers",
    "browser": "browser tabs",
}
ISOLATION = {
    "builtin": "operators only: no code from the job runs",
    "native-python": "none: the code runs as the agent's user, with its files and network",
    "managed-python": "dependencies only: pinned packages, but the same access as native",
    "isolated-oci": "a container: no network, read-only system files, not root, CPU/memory/process limits, only its own folders",
}
DATA_RULE = ("Friends' computers and friends' browser tabs read only datasets you marked shareable, and only for jobs that allow "
             "friends (security.allowedTrust: owned-and-friends).")


def tier(d) -> str:
    if getattr(d, "kind", "agent") == "browser":
        return "browser"
    return "owned" if d.owned else "friend"


def runtime_rule(kind: str, t: str) -> str | None:
    """None if a computer in tier t may run a job with this runtime, otherwise why not."""
    if kind == "builtin":
        return None
    if t == "browser":
        return "is a browser tab, which runs only the built-in operators"
    if t == "friend":
        if kind == "isolated-oci":
            return "is a friend's computer: a job's code doesn't run there, not even in a container"
        return "is a friend's computer, and your own code runs only on your own computers"
    return None


def admit(spec: dict, d, ds: dict | None, right: dict | None) -> str | None:
    """None if the trust rules let computer d run this job's partitions, otherwise why not.

    ds and right are the job's dataset rows (right: the dataset joined with), when it reads any.
    Whether the computer is able to (its operators, formats, environments) is checked elsewhere.
    """
    if not d.owned:
        if spec["security"]["allowedTrust"] == "owned-only":
            return "is a friend's computer, and this job runs on your own computers only"
        if ds is not None and ds.get("classification") != "shareable":
            return "is a friend's computer, and the dataset isn't marked shareable"
        if spec["inputs"].get("right") and (right is None or right.get("classification") != "shareable"):
            return "is a friend's computer, and the dataset to join with isn't marked shareable"
    return runtime_rule(spec["runtime"]["kind"], tier(d))


def table() -> dict:
    """The rules as data, for the API and the docs."""
    rules = [{"runtime": kind, "tier": t, "allowed": runtime_rule(kind, t) is None, "why": runtime_rule(kind, t)}
             for kind in ISOLATION for t in TIERS]
    return {"tiers": TIERS, "isolation": ISOLATION, "rules": rules, "data": DATA_RULE}
