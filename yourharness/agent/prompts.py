"""What an AI agent is told about its task."""

from __future__ import annotations


def build_prompt(title: str, spec: dict, feedback: str | None = None) -> str:
    scope = spec.get("scope") or ["**"]
    deny = spec.get("deny") or []
    checks = spec.get("checks") or []
    parts = [
        "You are working on one task in a Git repository. The current folder is the repository.",
        "",
        f"# Task: {title}",
        "",
        spec.get("instructions") or "",
        "",
        "# Rules",
    ]
    if scope != ["**"]:
        parts.append("- Change only files that match these paths (relative to the repository root): " + ", ".join(scope) + ".")
    if deny:
        parts.append("- Don't change files that match: " + ", ".join(deny) + ".")
    parts += [
        "- Don't commit, push, create branches or change Git settings. Leave your changes in the working tree; YourHarness commits them.",
        "- Don't read or change anything outside this folder, and don't print secrets, tokens or passwords.",
    ]
    if spec.get("kind") == "general" and spec.get("results_dir"):
        parts.append(f"- Put everything you make in {spec['results_dir']}/, with a short README.md there that explains it.")
    if checks:
        parts.append("- Before you finish, make sure these commands pass:")
        parts += [f"    {c}" for c in checks]
    parts += ["", "When you're done, reply with a short summary of what you changed and anything left to do."]
    if feedback:
        parts += ["", "# Your previous round", "", "The checks failed after your previous round. Fix the problems:", "", feedback]
    return "\n".join(parts).strip() + "\n"
