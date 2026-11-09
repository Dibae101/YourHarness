"""Agent tasks: AI agents, or plain commands, doing work on the pool's computers.

Each task runs in its own Git worktree on the computer it was given to. The
results are checked, combined and pushed to a branch of the run's repository.
"""

from .manager import TaskManager

__all__ = ["TaskManager"]
