# PyInstaller recipe for the YourHarness backend: one folder with a `harness` program inside.
# Build from the repository root:  pyinstaller desktop/backend.spec --noconfirm
import sys

from PyInstaller.utils.hooks import collect_submodules

a = Analysis(
    ["launcher.py"],
    pathex=[".."],
    # the source and web files ship as data too: the coordinator serves the agent bundle and the page from them
    datas=[("../yourharness", "yourharness")],
    hiddenimports=collect_submodules("yourharness") + ["qrcode"] + (["winpty"] if sys.platform == "win32" else []),
    excludes=["tkinter", "pytest"],
)
pyz = PYZ(a.pure)
exe = EXE(pyz, a.scripts, [], exclude_binaries=True, name="harness", console=True)
coll = COLLECT(exe, a.binaries, a.datas, name="backend")
