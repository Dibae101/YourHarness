"""Draws the three-wire logo as PNG files. Run from the repository root:  python desktop/make_icons.py"""
import struct, zlib
from pathlib import Path
import numpy as np

WIRES = [(-1, (0xE0, 0xA1, 0x00)), (0, (0x25, 0x63, 0xC9)), (1, (0xD1, 0x43, 0x3B))]
BG = (0x17, 0x21, 0x2B)


def render(n: int, rounded: bool = True) -> np.ndarray:
    ys, xs = (np.mgrid[0:n, 0:n] + 0.5) / n
    img = np.zeros((n, n, 4), dtype=np.float64)
    img[..., :3] = BG
    img[..., 3] = 1.0
    if rounded:
        r = 0.22
        cx, cy = np.clip(xs, r, 1 - r), np.clip(ys, r, 1 - r)
        d = np.hypot(xs - cx, ys - cy) - r
        img[..., 3] = np.clip(0.5 - d * n, 0, 1)
    t = np.clip((xs - 0.5) / 0.36, -1, 1)
    spread = 0.20 * (1 - (1 + np.cos(np.pi * t)) / 2)
    inside = (xs > 0.14) & (xs < 0.86)
    for side, color in WIRES:
        y = 0.5 + side * spread
        slope = np.abs(np.gradient(y, axis=1)) * n
        d = np.abs(ys - y) * n / np.sqrt(1 + slope ** 2) - 0.036 * n
        a = np.clip(0.5 - d, 0, 1) * np.clip((xs - 0.14) * n, 0, 1) * np.clip((0.86 - xs) * n, 0, 1) * inside
        for k in range(3):
            img[..., k] = img[..., k] * (1 - a) + color[k] * a
    return img


def write_png(path: Path, img: np.ndarray) -> None:
    rgba = np.clip(img.round(), 0, 255).astype(np.uint8)
    rgba[..., 3] = np.clip(np.where(img[..., 3] <= 1.0, img[..., 3] * 255, img[..., 3]).round(), 0, 255)
    raw = b"".join(b"\x00" + rgba[i].tobytes() for i in range(rgba.shape[0]))
    def chunk(tag, data):
        c = struct.pack(">I", len(data)) + tag + data
        return c + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    h, w = rgba.shape[:2]
    path.write_bytes(b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 6, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw, 9)) + chunk(b"IEND", b""))


def save(path: str, n: int, rounded: bool = True) -> None:
    img = render(n, rounded)
    rgba = img.copy()
    rgba[..., 3] = img[..., 3] * 255
    write_png(Path(path), rgba)


if __name__ == "__main__":
    save("desktop/icon.png", 1024)
    save("desktop/tray.png", 64)
    web = "yourharness/web"
    save(f"{web}/icon-512.png", 512)
    save(f"{web}/icon-192.png", 192)
    save(f"{web}/apple-touch-icon.png", 180, rounded=False)  # iOS rounds the corners itself
    print("icons written")
