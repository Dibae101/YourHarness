"""What this computer offers beyond processors and memory: graphics that jobs can use.

A GPU's name isn't enough to schedule on (the driver and the job's framework have to
work with it), so this only reports what can be detected cheaply and for certain:
NVIDIA GPUs that nvidia-smi lists with their driver, and Apple silicon's GPU (Metal).
Whether a framework such as PyTorch can actually use them is checked inside the
job's own environment when it is prepared (envs.py preflight).
"""

from __future__ import annotations

import platform
import shutil
import subprocess
import sys


def accelerators() -> list[dict]:
    out = []
    smi = shutil.which("nvidia-smi")
    if smi:
        try:
            res = subprocess.run([smi, "--query-gpu=name,memory.total,driver_version", "--format=csv,noheader,nounits"],
                                 capture_output=True, text=True, timeout=10)
            for line in res.stdout.splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 3 and parts[0]:
                    out.append({"kind": "cuda", "name": parts[0][:80], "memoryGB": round(float(parts[1]) / 1024, 1) if parts[1].replace(".", "").isdigit() else None,
                                "driver": parts[2][:40]})
        except (OSError, subprocess.SubprocessError, ValueError):
            pass
    if sys.platform == "darwin" and platform.machine() == "arm64":
        out.append({"kind": "metal", "name": "Apple silicon GPU", "memoryGB": None, "driver": "Metal"})
    return out
