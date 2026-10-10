"""What a computer can offer the pool: processors, memory, disk and graphics.

Standard library only, so the agent has no extra dependency for this.
"""

from __future__ import annotations

import os
import platform
import re
import shutil
import subprocess
import sys

GB = 1024 ** 3


def _run(cmd: list[str], timeout: float = 3.0) -> str:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
        return out.stdout if out.returncode == 0 else ""
    except (OSError, subprocess.SubprocessError):
        return ""


def is_wsl() -> bool:
    return sys.platform.startswith("linux") and "microsoft" in platform.release().lower()


def platform_name() -> str:
    if sys.platform == "darwin":
        return "Mac"
    if sys.platform.startswith("win"):
        return "Windows"
    if is_wsl():
        return "Windows (WSL)"
    if sys.platform.startswith("linux"):
        return "Linux"
    return platform.system() or "Computer"


def computer_name() -> str:
    """The name people know the computer by, such as "Darshan's MacBook Air"."""
    import socket

    name = ""
    if sys.platform == "darwin":
        name = _run(["scutil", "--get", "ComputerName"]).strip()
    elif sys.platform.startswith("win"):
        name = os.environ.get("COMPUTERNAME", "")
    name = name or socket.gethostname().split(".")[0]
    if is_wsl():
        name = f"{name} (Ubuntu)" if "ubuntu" in os_name().lower() else f"{name} (Linux)"
    return name[:40] or "Computer"


def os_name() -> str:
    """A readable OS name: macOS 15.1, Ubuntu 24.04.1 LTS, Windows 11."""
    if sys.platform == "darwin":
        return f"macOS {platform.mac_ver()[0]}".strip()
    if sys.platform.startswith("win"):
        return f"Windows {platform.release()}".strip()
    try:
        with open("/etc/os-release", encoding="utf-8") as f:
            for line in f:
                if line.startswith("PRETTY_NAME="):
                    return line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return "Linux"


def memory() -> tuple[int | None, int | None]:
    """Total and available memory in bytes, or None where the OS won't say."""
    if sys.platform.startswith("linux"):
        try:
            info = {}
            with open("/proc/meminfo", encoding="ascii") as f:
                for line in f:
                    key, _, rest = line.partition(":")
                    info[key] = int(rest.split()[0]) * 1024
            return info.get("MemTotal"), info.get("MemAvailable")
        except (OSError, ValueError, IndexError):
            return None, None
    if sys.platform == "darwin":
        total = _run(["sysctl", "-n", "hw.memsize"]).strip()
        vm = _run(["vm_stat"])
        avail = None
        if vm:
            page = re.search(r"page size of (\d+) bytes", vm)
            size = int(page.group(1)) if page else 4096
            pages = 0
            for key in ("Pages free", "Pages inactive", "Pages speculative", "Pages purgeable"):
                m = re.search(rf"{key}:\s+(\d+)", vm)
                if m:
                    pages += int(m.group(1))
            avail = pages * size
        return (int(total) if total.isdigit() else None), avail
    if sys.platform.startswith("win"):
        try:
            import ctypes

            class MemoryStatus(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            stat = MemoryStatus()
            stat.dwLength = ctypes.sizeof(MemoryStatus)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))  # type: ignore[attr-defined]
            return int(stat.ullTotalPhys), int(stat.ullAvailPhys)
        except (OSError, AttributeError):
            return None, None
    return None, None


def gpus() -> list[str]:
    found: list[str] = []
    smi = shutil.which("nvidia-smi") or ("/usr/lib/wsl/lib/nvidia-smi" if os.path.exists("/usr/lib/wsl/lib/nvidia-smi") else None)
    if smi:
        out = _run([smi, "--query-gpu=name,memory.total", "--format=csv,noheader"])
        for line in out.splitlines():
            name, _, mem = line.partition(",")
            if name.strip():
                found.append(f"{name.strip()} ({mem.strip()})" if mem.strip() else name.strip())
    if sys.platform == "darwin" and platform.machine() == "arm64":
        found.append("Apple GPU (Metal)")
    return found[:4]


def collect(disk_path: str | None = None) -> dict:
    total, avail = memory()
    try:
        disk_free = shutil.disk_usage(disk_path or os.path.expanduser("~")).free
    except OSError:
        disk_free = None
    return {
        "platform": platform_name(),
        "os": f"{platform.system()} {platform.release()}".strip(),
        "arch": platform.machine(),
        "cores": os.cpu_count() or 1,
        "memoryGB": round(total / GB, 1) if total else None,
        "memoryFreeGB": round(avail / GB, 1) if avail else None,
        "diskFreeGB": round(disk_free / GB, 1) if disk_free else None,
        "gpus": gpus(),
        "python": platform.python_version(),
    }


class CpuMeter:
    """CPU use across all cores since the previous reading, as a percentage."""

    def __init__(self):
        self.prev = self._ticks()

    @staticmethod
    def _ticks() -> tuple[float, float] | None:
        """(busy, total) time counters since boot, or None where unavailable."""
        try:
            if sys.platform.startswith("linux"):
                with open("/proc/stat", encoding="ascii") as f:
                    parts = [float(x) for x in f.readline().split()[1:]]
                idle = parts[3] + (parts[4] if len(parts) > 4 else 0)
                return sum(parts) - idle, sum(parts)
            if sys.platform == "darwin":
                import ctypes

                lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
                lib.mach_host_self.restype = ctypes.c_uint
                ticks = (ctypes.c_uint * 4)()  # user, system, idle, nice
                count = ctypes.c_uint(4)
                if lib.host_statistics(lib.mach_host_self(), 3, ctypes.byref(ticks), ctypes.byref(count)) != 0:  # HOST_CPU_LOAD_INFO
                    return None
                user, system, idle, nice = (float(t) for t in ticks)
                return user + system + nice, user + system + nice + idle
            if sys.platform.startswith("win"):
                import ctypes

                idle, kernel, user = (ctypes.c_ulonglong(), ctypes.c_ulonglong(), ctypes.c_ulonglong())
                if not ctypes.windll.kernel32.GetSystemTimes(ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)):  # type: ignore[attr-defined]
                    return None
                total = float(kernel.value + user.value)  # kernel time includes idle time
                return total - idle.value, total
        except (OSError, ValueError, IndexError, AttributeError):
            return None
        return None

    def sample(self) -> float | None:
        now = self._ticks()
        prev, self.prev = self.prev, now
        if not now or not prev or now[1] <= prev[1]:
            return None
        return round(max(0.0, min(100.0, (now[0] - prev[0]) / (now[1] - prev[1]) * 100)), 1)
