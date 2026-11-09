"""Git work on the coordinator.

The coordinator keeps a bare copy of each repository. From it, it makes the bundle
a computer starts a task from, imports the bundle the computer sends back, checks
what it changes, cherry-picks verified results onto the run's branch, and pushes
that branch. Workers never talk to GitHub, and the GitHub token is passed to git
through its environment, never on a command line or in a remote URL.
"""

from __future__ import annotations

import asyncio
import base64
import os
import re
import secrets
from pathlib import Path

GIT_TIMEOUT = 900.0
MAX_DIFF = 20 * 1024 * 1024


class GitError(Exception):
    pass


async def git_rc(*args, cwd=None, env: dict | None = None, timeout: float = GIT_TIMEOUT) -> tuple[int, str, str]:
    full_env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C", "GIT_LITERAL_PATHSPECS": "1", **(env or {})}
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", *map(str, args), cwd=cwd, env=full_env,
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        raise GitError("Git isn't installed on the coordinator's computer. Install it, then try again.") from None
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise GitError(f"git {args[0] if args else ''} took more than {int(timeout)} s and was stopped.") from None
    return proc.returncode, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")


async def git(*args, **kw) -> str:
    code, out, err = await git_rc(*args, **kw)
    if code != 0:
        raise GitError(err.strip() or out.strip() or f"git exited with code {code}")
    return out


def github_git_base() -> str:
    return os.environ.get("HARNESS_GITHUB_GIT", "https://github.com").rstrip("/")


class GitOps:
    def __init__(self, root: Path, secrets_store):
        self.root = Path(root)
        self.secrets = secrets_store

    # ------------------------------------------------------------------ where and how
    def mirror(self, repo: dict) -> Path:
        return self.root / "repos" / f"{repo['id']}.git"

    def remote_url(self, repo: dict) -> str:
        if repo["kind"] == "github":
            return f"{github_git_base()}/{repo['name']}.git"
        return repo["name"]

    def auth_env(self, repo: dict) -> dict:
        if repo["kind"] != "github":
            return {}  # a local repository uses the computer's own Git setup
        # never ask the system keychain for GitHub credentials; only the token below counts
        env = {"GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "credential.helper", "GIT_CONFIG_VALUE_0": ""}
        token = self.secrets.get("github_token")
        if token:
            basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
            env.update({"GIT_CONFIG_COUNT": "2", "GIT_CONFIG_KEY_1": f"http.{github_git_base()}/.extraheader",
                        "GIT_CONFIG_VALUE_1": f"AUTHORIZATION: basic {basic}"})
        return env

    def explain(self, err: Exception, repo: dict, branch: str | None = None) -> str:
        text = self.secrets.redact(str(err))
        low = text.lower()
        name = repo["name"]
        if repo["kind"] == "github":
            if "authentication failed" in low or "could not read username" in low or "403" in low or "401" in low:
                return f"GitHub refused the token for {name}. Check that it can read and write this repository's contents."
            if "repository not found" in low or "not found" in low and "repository" in low:
                return f"GitHub can't find {name}, or the token can't see it."
        if "non-fast-forward" in low or "fetch first" in low or "rejected" in low and branch:
            return f"The branch {branch} already has other commits on the remote. Pick a different results branch."
        if "does not appear to be a git repository" in low or "not a git repository" in low:
            return f"{name} isn't a Git repository the coordinator can read."
        return text.strip()[-800:]

    # ------------------------------------------------------------------ the bare copy
    async def fetch(self, repo: dict) -> Path:
        path = self.mirror(repo)
        url = self.remote_url(repo)
        if not (path / "HEAD").exists():
            path.mkdir(parents=True, exist_ok=True)
            await git("init", "--bare", "-q", path)
            await git("-C", path, "remote", "add", "origin", url)
            # only branches: refs/yh/* (bases, task results, run branches) must survive --prune
            await git("-C", path, "config", "remote.origin.fetch", "+refs/heads/*:refs/heads/*")
        else:
            await git("-C", path, "remote", "set-url", "origin", url)
        try:
            await git("-C", path, "fetch", "--prune", "--no-tags", "-q", "origin", env=self.auth_env(repo))
        except GitError as e:
            raise GitError(self.explain(e, repo)) from None
        return path

    async def default_branch(self, repo: dict) -> str | None:
        try:
            out = await git("ls-remote", "--symref", self.remote_url(repo), "HEAD", env=self.auth_env(repo), timeout=120)
        except GitError as e:
            raise GitError(self.explain(e, repo)) from None
        m = re.search(r"^ref: refs/heads/(\S+)\s+HEAD", out, re.M)
        return m.group(1) if m else None

    async def branches(self, repo: dict) -> list[str]:
        out = await git("-C", self.mirror(repo), "for-each-ref", "--format=%(refname:short)", "refs/heads")
        return [b for b in out.splitlines() if b]

    async def resolve(self, repo: dict, branch: str) -> str:
        code, out, _ = await git_rc("-C", self.mirror(repo), "rev-parse", "--verify", "-q", f"refs/heads/{branch}^{{commit}}")
        if code != 0 or not out.strip():
            raise GitError(f"There's no branch called {branch} in {repo['name']}.")
        return out.strip()

    async def run_head(self, repo: dict, run_id: str) -> str | None:
        code, out, _ = await git_rc("-C", self.mirror(repo), "rev-parse", "--verify", "-q", f"refs/yh/runs/{run_id}")
        return out.strip() if code == 0 and out.strip() else None

    def bundle_path(self, sha: str) -> Path:
        return self.root / "bundles" / f"{sha}.bundle"

    async def bundle_for(self, repo: dict, sha: str) -> Path:
        """A bundle with everything up to commit sha, the starting point a computer downloads."""
        out = self.bundle_path(sha)
        if out.exists():
            return out
        ref = f"refs/yh/base/{sha}"
        await git("-C", self.mirror(repo), "update-ref", ref, sha)
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_name(f".{sha}.{secrets.token_hex(3)}.bundle")
        await git("-C", self.mirror(repo), "bundle", "create", tmp, ref)
        os.replace(tmp, out)
        return out

    # ------------------------------------------------------------------ what a computer sent back
    async def import_result(self, repo: dict, task_id: str, attempt: int, bundle: Path) -> str:
        path = self.mirror(repo)
        ref = f"refs/yh/tasks/{task_id}/{attempt}"
        try:
            await git("-C", path, "bundle", "verify", "-q", bundle)
            await git("-C", path, "fetch", "-q", bundle, f"+refs/yh/result/{task_id}-{attempt}:{ref}")
        except GitError as e:
            raise GitError(f"The result couldn't be read: {str(e)[-300:]}") from None
        return ref

    async def inspect(self, repo: dict, start: str, ref: str) -> dict:
        """What a task's commits change: files, modes, sizes, link targets and the diff text."""
        path = self.mirror(repo)
        code, _, _ = await git_rc("-C", path, "merge-base", "--is-ancestor", start, ref)
        if code != 0:
            return {"error": "The result doesn't start from the commit the task was given."}
        count = int((await git("-C", path, "rev-list", "--count", f"{start}..{ref}")).strip() or 0)
        raw = await git("-C", path, "diff", "--name-status", "-z", "--no-renames", start, ref)
        parts = raw.split("\0")
        changes: list[tuple[str, str]] = []
        for i in range(0, len(parts) - 1, 2):
            if parts[i]:
                changes.append((parts[i][0], parts[i + 1]))
        entries: dict[str, dict] = {}
        present = [p for s, p in changes if s != "D"]
        for i in range(0, len(present), 200):
            out = await git("-C", path, "ls-tree", "-r", "-l", "-z", ref, "--", *present[i:i + 200])
            for item in out.split("\0"):
                if not item or "\t" not in item:
                    continue
                meta, name = item.split("\t", 1)
                mode, _kind, oid, size = (meta.split() + ["-"])[:4]
                entries[name] = {"mode": mode, "oid": oid, "size": int(size) if size.isdigit() else 0}
        symlinks = {}
        for name, e in entries.items():
            if e["mode"] == "120000":
                symlinks[name] = (await git("-C", path, "cat-file", "blob", e["oid"])).strip()
        diff = await git("-C", path, "diff", "--no-color", "--no-ext-diff", "-U0", "--no-renames", start, ref)
        return {"count": count, "changes": changes, "entries": entries, "symlinks": symlinks, "diff": diff[:MAX_DIFF]}

    async def diff_text(self, repo: dict, start: str, ref: str, limit: int = 1_000_000) -> str:
        path = self.mirror(repo)
        stat = await git("-C", path, "diff", "--stat", "--no-color", start, ref)
        patch = await git("-C", path, "diff", "--no-color", "--no-ext-diff", start, ref)
        text = stat + "\n" + patch
        return text if len(text) <= limit else text[:limit] + "\n\n[The diff is longer than this; see the branch for the rest.]\n"

    # ------------------------------------------------------------------ combining and pushing
    async def integrate(self, repo: dict, run_id: str, base: str, tasks: list[dict]) -> dict:
        """Cherry-pick each task's commits onto the run's branch so far, in order.

        tasks: [{"id", "ref", "start"}]. A task that conflicts with what is already
        there is left out and reported; the others still go in.
        """
        path = self.mirror(repo)
        start = await self.run_head(repo, run_id) or base
        work = self.root / "work" / f"{run_id}-{secrets.token_hex(3)}"
        work.parent.mkdir(parents=True, exist_ok=True)
        await git_rc("-C", path, "worktree", "prune")
        await git("-C", path, "worktree", "add", "-q", "--detach", work, start)
        ident = ["-c", "user.name=YourHarness", "-c", "user.email=yourharness@localhost", "-c", "commit.gpgsign=false"]
        merged, conflicts = [], {}
        try:
            for t in tasks:
                code, out, err = await git_rc("-C", work, *ident, "cherry-pick", "--allow-empty", "--keep-redundant-commits",
                                              f"{t['start']}..{t['ref']}")
                if code == 0:
                    merged.append(t["id"])
                    continue
                _, unmerged, _ = await git_rc("-C", work, "diff", "--name-only", "--diff-filter=U")
                await git_rc("-C", work, "cherry-pick", "--abort")
                files = [f for f in unmerged.splitlines() if f]
                conflicts[t["id"]] = files or [(err or out).strip()[-300:] or "the changes don't apply"]
            head = (await git("-C", work, "rev-parse", "HEAD")).strip()
            if merged or not await self.run_head(repo, run_id):
                await git("-C", path, "update-ref", f"refs/yh/runs/{run_id}", head)
            return {"head": head, "merged": merged, "conflicts": conflicts}
        finally:
            await git_rc("-C", path, "worktree", "remove", "--force", work)

    async def push(self, repo: dict, run_id: str, branch: str) -> None:
        if branch in ("main", "master", "mainline", "trunk"):  # also refused when the run is made; checked again here
            raise GitError(f"Refusing to push to {branch}.")
        try:
            await git("-C", self.mirror(repo), "push", "--porcelain", self.remote_url(repo),
                      f"refs/yh/runs/{run_id}:refs/heads/{branch}", env=self.auth_env(repo))
        except GitError as e:
            raise GitError(self.explain(e, repo, branch)) from None
