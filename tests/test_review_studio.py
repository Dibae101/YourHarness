"""Exercise scoped invitations, retries and durable review state over real HTTP."""

import asyncio
import io
import json
import time
import zipfile

import aiohttp
from PIL import Image

from yourharness.review import media
from yourharness.review.plan import estimate


def test_estimate_requests_minimum_help_or_keeps_work_local():
    p = estimate(100, [1, 1, 1], 1_000_000, 85, 0.5, 20)
    assert p["helperFrames"] == 15 and p["combinedSeconds"] == 85 and p["meetsDeadline"]
    assert estimate(100, [1], 1_000_000, 120, 0.5, 20)["helperFrames"] == 0
    assert estimate(1, [1], 1_000_000, 0.5, 0.1, 0.01)["helperFrames"] == 0


def test_review_consent_reclaim_and_restart(fresh_coord):
    c = fresh_coord
    owner = {"Authorization": "Bearer " + c.client().token}

    async def main():
        async with aiohttp.ClientSession() as h:
            url = c.base + "/api/reviews"
            assert (await h.post(url + "?kind=demo")).status == 401
            r = await h.post(
                url, headers=owner, params={"kind": "demo", "name": "Review", "deadline": str(time.time() + 300)}
            )
            assert r.status == 201, await r.text()
            job = url + "/" + (await r.json())["id"]
            assert (await h.post(job + "/action", headers=owner, json={"action": "start"})).status == 200
            inv = await (
                await h.post(job + "/invites", headers=owner, json={"frames": 1, "minutes": 2, "workers": 1})
            ).json()
            ih = {"X-Review-Invite": inv["token"]}
            assert (await h.get(url + "/invitation", headers=ih)).status == 200
            answer = await (
                await h.post(url + "/accept", headers=ih, json={"name": "Guest", "minutes": 1, "workers": 1})
            ).json()
            guest = {"X-Review-Session": answer["session"]}
            assert (await h.get(url + "/invitation", headers=ih)).status == 410
            assert (await h.get(job, headers=guest)).status == 401
            a = await (await h.post(job + "/claim", headers=guest)).json()
            ticket = job + "/lease/" + a["ticket"]
            assert (await h.get(ticket, headers=guest)).status == 200
            assert "ticket" not in await (await h.post(job + "/claim", headers=guest)).json()
            assert (await h.post(job + "/release", headers=guest)).status == 200
            assert (await h.get(ticket, headers=guest)).status == 409
            again = await (await h.post(job + "/claim", headers=guest)).json()
            assert again["frame"] == a["frame"] and again["ticket"] != a["ticket"]
            result = job + "/lease/" + again["ticket"]
            data = io.BytesIO()
            Image.new("RGB", (again["width"], again["height"])).save(data, "JPEG")
            upload_headers = {**guest, "X-Frame-Seconds": "0.25"}
            assert (await h.put(result, headers=upload_headers, data=b"invalid")).status == 400
            assert (await h.put(result, headers=upload_headers, data=data.getvalue())).status == 200
            assert (await h.put(result, headers=upload_headers, data=data.getvalue())).status == 409
            assert (await (await h.post(job + "/claim", headers=guest)).json())["status"] == "fulfilled"
            await h.post(job + "/action", headers=owner, json={"action": "revoke"})
            assert (await h.post(job + "/claim", headers=guest)).status == 403
            await h.post(job + "/claim", headers=owner)
            return job.rsplit("/", 1)[1]

    jid = asyncio.run(main())
    c.proc.stop()
    from conftest import start_coordinator

    again = start_coordinator(c.tmp)
    try:
        client = again.client()
        state = client.request("GET", f"/api/reviews/{jid}")
        assert state["done"] == 1 and state["status"] == "rendering"
        next_frame = client.request("POST", f"/api/reviews/{jid}/claim")
        assert next_frame["frame"] == 1
    finally:
        again.proc.stop()


def test_mesh_package_rejects_invalid_geometry(tmp_path):
    dest = tmp_path / "in"
    dest.mkdir()
    package = tmp_path / "scene.yhreview"
    with zipfile.ZipFile(package, "w") as z:
        z.writestr(
            "manifest.json", json.dumps({"format": "yourharness.mesh.v1", "frames": 1, "fps": 24, "aspect": 1})
        )
        z.writestr("frames/00000.mesh", b"invalid")
    import pytest

    with pytest.raises(ValueError, match="geometry"):
        media.mesh_package(package, dest, 640)
