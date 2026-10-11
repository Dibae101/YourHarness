"""One-line agent install, served by the coordinator itself.

On the computer that should join (for the workspace host: Ubuntu inside WSL2 on
the Windows PC):

    curl -fsSL http://<coordinator>/install.sh | bash -s -- --join '<join link>' --workspace

The script needs Python 3.10+ and internet access for two packages (aiohttp, numpy).
The agent code itself comes from the coordinator, so every agent runs the same version.
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent.parent

_tarball: bytes | None = None

INSTALL_SH = r"""#!/usr/bin/env bash
# YourHarness agent installer.
set -euo pipefail
SERVER="__SERVER__"
DIR="${YOURHARNESS_HOME:-$HOME/.yourharness}"

say() { printf '%s\n' "$*"; }
fail() { printf 'YourHarness: %s\n' "$*" >&2; exit 1; }

command -v python3 >/dev/null 2>&1 || fail "Python 3 is missing. On Ubuntu or WSL run: sudo apt install -y python3 python3-venv"
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' || fail "Python 3.10 or newer is required (found $(python3 -V 2>&1))."
command -v curl >/dev/null 2>&1 || fail "curl is missing. On Ubuntu or WSL run: sudo apt install -y curl"

mkdir -p "$DIR"
if [ ! -x "$DIR/venv/bin/python" ]; then
  say "Creating a Python environment in $DIR/venv"
  python3 -m venv "$DIR/venv" >/dev/null 2>&1 || { rm -rf "$DIR/venv"; fail "Python can't create a virtual environment. On Ubuntu or WSL run: sudo apt install -y python3-venv   then run this command again."; }
fi
say "Installing aiohttp and numpy"
"$DIR/venv/bin/python" -m pip install --quiet --disable-pip-version-check --upgrade "aiohttp>=3.9" "numpy>=1.24"

say "Downloading the agent from $SERVER"
rm -rf "$DIR/src.new" && mkdir -p "$DIR/src.new"
curl -fsSL "$SERVER/agent.tar.gz" | tar -xz -C "$DIR/src.new"
rm -rf "$DIR/src" && mv "$DIR/src.new" "$DIR/src"

mkdir -p "$DIR/bin"
cat > "$DIR/bin/harness" <<EOF
#!/usr/bin/env bash
export PYTHONPATH="$DIR/src\${PYTHONPATH:+:\$PYTHONPATH}"
exec "$DIR/venv/bin/python" -m yourharness "\$@"
EOF
chmod +x "$DIR/bin/harness"

if ! grep -qs 'yourharness/bin' "$HOME/.bashrc"; then
  printf '\nexport PATH="%s/bin:$PATH"  # yourharness\n' "$DIR" >> "$HOME/.bashrc"
fi
if ! command -v tmux >/dev/null 2>&1; then
  say "Tip: install tmux (sudo apt install -y tmux) so every device shares one workspace session that survives disconnects."
fi

say "Installed. Run it again any time with: $DIR/bin/harness agent"
if [ "$#" -gt 0 ]; then
  exec "$DIR/bin/harness" agent --server "$SERVER" "$@"
fi
"""


INSTALL_PS1 = r"""# YourHarness setup for Windows. Run in PowerShell:  irm __SERVER__/install.ps1 | iex
$ErrorActionPreference = 'Stop'
$server = '__SERVER__'
Write-Host 'YourHarness: setting up the Ubuntu workspace on this PC.'
$distros = ''
try { $distros = (wsl.exe -l -q 2>$null) -join '' } catch { }
$distros = $distros -replace "`0", ''
if ($distros -notmatch 'Ubuntu') {
  Write-Host 'Ubuntu is not installed yet. Installing it now. This needs an administrator PowerShell.'
  wsl.exe --install -d Ubuntu
  Write-Host ''
  Write-Host 'Restart the PC if Windows asks you to. Open Ubuntu once from the Start menu to create your Linux user name and password.'
  Write-Host 'Then run this same command again.'
  return
}
$code = Read-Host 'Join code (shown where the coordinator is running)'
if ($code -notmatch '^[0-9]{4}$') { Write-Host 'The join code is 4 digits.'; return }
Write-Host 'Installing the agent inside Ubuntu. It may ask for your Linux password once.'
Write-Host 'Keep this window open while you want this PC in the pool.'
wsl.exe -d Ubuntu -- bash -lc "sudo apt-get update -qq && sudo apt-get install -y -qq python3-venv tmux curl && curl -fsSL $server/install.sh | bash -s -- --join $code --workspace"
"""


def install_ps1(server: str) -> str:
    return INSTALL_PS1.replace("__SERVER__", server.rstrip("/"))


def install_script(server: str) -> str:
    return INSTALL_SH.replace("__SERVER__", server.rstrip("/"))


def agent_tarball() -> bytes:
    """The yourharness package as a .tar.gz, built once per coordinator start."""
    global _tarball
    if _tarball is None:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for path in sorted(PACKAGE_DIR.rglob("*")):
                rel = path.relative_to(PACKAGE_DIR.parent)
                if "__pycache__" in rel.parts or path.suffix in (".pyc", ".map") or not path.is_file():
                    continue
                tar.add(path, arcname=str(rel))
        _tarball = buf.getvalue()
    return _tarball
