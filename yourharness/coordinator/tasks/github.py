"""The GitHub REST calls the coordinator makes with its own token.

Only the coordinator holds the token. Computers doing the work never see it and
never talk to GitHub.
"""

from __future__ import annotations

import json
import os

import aiohttp


class GitHubError(Exception):
    def __init__(self, message: str, status: int = 0, body=None):
        super().__init__(message)
        self.status = status
        self.body = body


class GitHub:
    def __init__(self, secrets_store):
        self.secrets = secrets_store
        self.api = os.environ.get("HARNESS_GITHUB_API", "https://api.github.com").rstrip("/")

    @property
    def connected(self) -> bool:
        return bool(self.secrets.get("github_token"))

    async def _req(self, method: str, path: str, *, token: str | None = None, body: dict | None = None, params: dict | None = None):
        token = token or self.secrets.get("github_token")
        if not token:
            raise GitHubError("Connect GitHub first: add a token in Agents → GitHub.")
        headers = {
            "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "YourHarness",
        }
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as http:
                async with http.request(method, self.api + path, headers=headers, json=body, params=params) as res:
                    try:
                        data = await res.json(content_type=None)
                    except (ValueError, aiohttp.ContentTypeError):
                        data = None
                    if res.status >= 400:
                        msg = data.get("message") if isinstance(data, dict) else None
                        if res.status == 401:
                            msg = "the token isn't valid (it may have expired)"
                        raise GitHubError(f"GitHub answered {res.status}: {msg or res.reason}", res.status, data)
                    return data
        except aiohttp.ClientError as e:
            raise GitHubError(f"Couldn't reach GitHub ({e}).") from None

    async def user(self, token: str) -> str:
        data = await self._req("GET", "/user", token=token)
        return str(data.get("login") or "")

    async def repos(self) -> list[dict]:
        data = await self._req("GET", "/user/repos", params={"per_page": "100", "sort": "updated"})
        out = []
        for r in data or []:
            perms = r.get("permissions") or {}
            out.append({"name": r.get("full_name"), "private": bool(r.get("private")), "defaultBranch": r.get("default_branch"),
                        "canPush": bool(perms.get("push", True))})
        return out

    async def branches(self, full_name: str) -> list[str]:
        data = await self._req("GET", f"/repos/{full_name}/branches", params={"per_page": "100"})
        return [b.get("name") for b in data or [] if b.get("name")]

    async def open_pr(self, full_name: str, head: str, base: str, title: str, body: str) -> str:
        payload = {"title": title, "head": head, "base": base, "body": body, "draft": True}
        try:
            data = await self._req("POST", f"/repos/{full_name}/pulls", body=payload)
        except GitHubError as e:
            text = json.dumps(e.body or {}).lower()
            if e.status == 422 and "draft" in text:  # draft PRs aren't available on every plan
                data = await self._req("POST", f"/repos/{full_name}/pulls", body={**payload, "draft": False})
            elif e.status == 422 and "already exists" in text:
                owner = full_name.split("/")[0]
                found = await self._req("GET", f"/repos/{full_name}/pulls", params={"head": f"{owner}:{head}", "state": "open"})
                if found:
                    return found[0].get("html_url") or ""
                raise
            else:
                raise
        return (data or {}).get("html_url") or ""

    async def update_pr(self, full_name: str, url: str, body: str) -> None:
        """Rewrite a pull request's description (after new results or final checks)."""
        number = url.rstrip("/").rsplit("/", 1)[-1]
        if not number.isdigit():
            raise GitHubError(f"Can't tell the pull request number from {url}.")
        await self._req("PATCH", f"/repos/{full_name}/pulls/{number}", body={"body": body})
