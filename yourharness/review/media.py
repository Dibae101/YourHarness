"""Review package import and MP4 encoding. All paths on disk are assigned locally."""

import array
import io
import json
import math
import os
import shutil
import struct
import subprocess
import sys
import zipfile

from PIL import Image, ImageOps

MAX_BYTES = 128 * 1024**2
MAX_FRAMES = 1800
MAX_TRIANGLES = 24000
TRIANGLE = struct.Struct("<9f3Bx")


def dimensions(aspect, width):
    if not math.isfinite(aspect) or not 0.1 <= aspect <= 10:
        raise ValueError("Unsupported camera aspect ratio.")
    return [max(2, round(width * min(1, aspect) / 2) * 2), max(2, round(width * min(1, 1 / aspect) / 2) * 2)]


def mesh_package(path, dest, width):
    with zipfile.ZipFile(path) as z:
        entries = z.infolist()
        if len(entries) > MAX_FRAMES + 1 or sum(i.file_size for i in entries) > MAX_BYTES:
            raise ValueError("Package exceeds the review size limit.")
        if z.getinfo("manifest.json").file_size > 16384:
            raise ValueError("Manifest is too large.")
        m = json.loads(z.read("manifest.json"))
        count, fps = m["frames"], m["fps"]
        if (
            m["format"] != "yourharness.mesh.v1"
            or type(count) is not int
            or not 1 <= count <= MAX_FRAMES
            or not 1 <= fps <= 120
        ):
            raise ValueError("Unsupported animation package.")
        names = [f"frames/{i:05}.mesh" for i in range(count)]
        if sorted(z.namelist()) != sorted(["manifest.json", *names]):
            raise ValueError("Package contents differ from its manifest.")
        for i, name in enumerate(names):
            size = z.getinfo(name).file_size
            if size % TRIANGLE.size or size > MAX_TRIANGLES * TRIANGLE.size:
                raise ValueError("A frame exceeds the geometry limit.")
            data = z.read(name)
            # Nine finite coordinates and a packed colour per triangle.
            values = array.array("f")
            values.frombytes(data)
            if sys.byteorder != "little":
                values.byteswap()
            if any(not math.isfinite(v) or abs(v) > 1e12 for p, v in enumerate(values) if p % 10 != 9):
                raise ValueError("Invalid projected geometry.")
            (dest / str(i)).write_bytes(data)
    return count, float(fps), dimensions(m["aspect"], width)


def import_images(paths, dest, width):
    aspect = 1
    total = 0
    for i, path in enumerate(paths):
        with Image.open(path) as original:
            if original.format not in ("JPEG", "PNG") or original.width * original.height > 12_000_000:
                raise ValueError("Use JPEG or PNG images up to 12 megapixels each.")
            rgb = ImageOps.exif_transpose(original).convert("RGB")
            if i == 0:
                aspect = rgb.width / rgb.height
            rgb.save(dest / str(i), format="JPEG", quality=92)
        total += (dest / str(i)).stat().st_size
        if total > MAX_BYTES:
            raise ValueError("The normalized images exceed the review size limit.")
    return dimensions(aspect, width)


def demo(dest, width):
    """Small animated turntable. Use the same packet format as the Blender exporter."""
    faces = [(0, 1, 3, 2), (4, 6, 7, 5), (0, 4, 5, 1), (2, 3, 7, 6), (0, 2, 6, 4), (1, 5, 7, 3)]
    for frame in range(72):
        t = 2 * math.pi * frame / 72
        data = bytearray()
        for k in range(9):
            a = t + k * math.tau / 9
            cx, cz = math.cos(a) * 1.65, math.sin(a) * 1.65
            h = 0.48 + 0.32 * math.sin(a * 2 + t)
            pts = []
            for j in range(8):
                x = cx + ((j & 1) - 0.5) * 0.78
                y = (bool(j & 2) * 2 - 1) * h
                z = cz + (bool(j & 4) - 0.5) * 0.78
                depth = 6 - z * 0.8 - y * 0.3
                pts.append((0.5 + x / depth * 0.86, 0.52 - (y - z * 0.42) / depth * 1.45, 1 / depth))
            for fi, face in enumerate(faces):
                tint = (186, 237, 99) if k % 3 else (115, 172, 240)
                color = [int(v * (0.54 + fi * 0.085)) for v in tint]
                for ids in ((0, 1, 2), (0, 2, 3)):
                    data.extend(TRIANGLE.pack(*(v for ix in ids for v in pts[face[ix]]), *color))
        (dest / str(frame)).write_bytes(data)
    return 72, 24, dimensions(16 / 9, width)


def save_result(data, path, width, height):
    with Image.open(io.BytesIO(data)) as im:
        if im.format != "JPEG" or im.size != (width, height):
            raise ValueError("The result dimensions or format do not match the job.")
        im.convert("RGB").save(path, "JPEG", quality=92)


def encoder():
    exe = shutil.which("ffmpeg")
    if exe:
        return exe
    import imageio_ffmpeg

    return imageio_ffmpeg.get_ffmpeg_exe()


def encode(folder, fps, count):
    out = folder / "encoding.mp4"
    cmd = [
        encoder(),
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-framerate",
        str(fps),
        "-start_number",
        "0",
        "-i",
        str(folder / "out" / "%05d.jpg"),
        "-frames:v",
        str(count),
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-threads",
        "2",
        "-movflags",
        "+faststart",
        str(out),
    ]
    subprocess.run(cmd, capture_output=True, check=True, timeout=600)
    os.replace(out, folder / "review.mp4")
