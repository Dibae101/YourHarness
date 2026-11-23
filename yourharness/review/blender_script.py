"""Blender add-on exporting camera-space triangles for YourHarness review cuts."""

bl_info = {
    "name": "YourHarness motion review",
    "category": "Import-Export",
    "version": (1, 0, 0),
    "blender": (4, 2, 0),
    "location": "File > Export > YourHarness motion review",
}
import json
import os
import struct
import tempfile
import zipfile

import bpy
from bpy_extras.io_utils import ExportHelper
from bpy_extras.object_utils import world_to_camera_view
from mathutils import Vector

TRI = struct.Struct("<9f3Bx")


def export_review(context, filename):
    scene = context.scene
    camera = scene.camera
    if not camera or camera.data.type not in ("PERSP", "ORTHO"):
        raise ValueError("Choose a perspective or orthographic camera.")
    frames = range(scene.frame_start, scene.frame_end + 1, scene.frame_step)
    if not 1 <= len(frames) <= 1800:
        raise ValueError("Export up to 1800 frames.")
    r = scene.render
    fps = r.fps / (r.fps_base * scene.frame_step)
    if not 1 <= fps <= 120:
        raise ValueError("Review playback supports 1 to 120 fps.")
    manifest = {
        "format": "yourharness.mesh.v1",
        "frames": len(frames),
        "fps": fps,
        "aspect": (r.resolution_x * r.pixel_aspect_x) / (r.resolution_y * r.pixel_aspect_y),
    }
    old, total = scene.frame_current, 0
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(filename)))
    os.close(fd)
    try:
        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, compresslevel=2) as z:
            z.writestr("manifest.json", json.dumps(manifest))
            for index, frame in enumerate(frames):
                scene.frame_set(frame)
                graph = context.evaluated_depsgraph_get()
                camera_eval = camera.evaluated_get(graph)
                data = bytearray()
                for inst in graph.object_instances:
                    obj = inst.object
                    if obj.type != "MESH" or obj.hide_render or not inst.show_self:
                        continue
                    mesh = obj.data
                    mesh.calc_loop_triangles()
                    mx = inst.matrix_world
                    projected = [world_to_camera_view(scene, camera_eval, mx @ v.co) for v in mesh.vertices]
                    normal_matrix = mx.to_3x3().inverted_safe().transposed()
                    for tri in mesh.loop_triangles:
                        p = [projected[i] for i in tri.vertices]
                        if (
                            min(v.z for v in p) < camera_eval.data.clip_start
                            or min(v.z for v in p) > camera_eval.data.clip_end
                        ):
                            continue
                        if any(max(v[axis] for v in p) < 0 or min(v[axis] for v in p) > 1 for axis in (0, 1)):
                            continue
                        mat = (
                            mesh.materials[tri.material_index]
                            if tri.material_index < len(mesh.materials)
                            else None
                        )
                        color = mat.diffuse_color if mat else obj.color
                        normal = (normal_matrix @ tri.normal).normalized()
                        light = 0.4 + 0.6 * max(0, normal.dot(Vector((0.4, -0.5, 0.75)).normalized()))
                        rgb = [round(255 * min(1, max(0, c * light)) ** (1 / 2.2)) for c in color[:3]]
                        coords = [(v.x, 1 - v.y, 1 / v.z if camera_eval.data.type == "PERSP" else -v.z) for v in p]
                        data.extend(TRI.pack(*(v for point in coords for v in point), *rgb))
                        if len(data) > 24000 * TRI.size:
                            raise ValueError(
                                f"Frame {frame} exceeds 24,000 visible triangles. Simplify the scene."
                            )
                total += len(data)
                if total > 127 * 1024**2:
                    raise ValueError("Export a shorter or simpler sequence (128 MiB package limit).")
                z.writestr(f"frames/{index:05}.mesh", data)
        os.replace(tmp, filename)
    finally:
        scene.frame_set(old)
        if os.path.exists(tmp):
            os.unlink(tmp)


class ExportReview(bpy.types.Operator, ExportHelper):
    bl_idname = "export_scene.yourharness_review"
    bl_label = "Export motion review"
    filename_ext = ".yhreview"

    def execute(self, context):
        try:
            export_review(context, self.filepath)
        except (ValueError, OSError) as exc:
            self.report({"ERROR"}, str(exc))
            return {"CANCELLED"}
        return {"FINISHED"}


def menu(self, context):
    self.layout.operator(ExportReview.bl_idname, text="YourHarness motion review (.yhreview)")


def register():
    bpy.utils.register_class(ExportReview)
    bpy.types.TOPBAR_MT_file_export.append(menu)


def unregister():
    bpy.types.TOPBAR_MT_file_export.remove(menu)
    bpy.utils.unregister_class(ExportReview)
