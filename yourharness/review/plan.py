"""Estimate a two-device split for a fixed set of independent review frames."""

import math
import statistics


def estimate(frames, samples, wire_bytes, budget, helper_seconds=None, upload_mbps=None):
    if not samples:
        return {"framesLeft": frames, "localSeconds": None}
    if any(not math.isfinite(t) or t <= 0 for t in samples):
        raise ValueError("Frame times must be positive finite seconds.")
    typical = statistics.median(samples)
    result = {
        "framesLeft": frames,
        "localSeconds": round(frames * typical, 2),
        "localRange": [round(frames * min(samples), 2), round(frames * max(samples), 2)],
        "budgetSeconds": round(budget),
        "helperFrames": 0,
    }
    if helper_seconds is None and upload_mbps is None:
        return result
    if any(x is None or not math.isfinite(x) or x <= 0 for x in (helper_seconds, upload_mbps)):
        raise ValueError("Supply positive helper frame seconds and connection Mbps together.")
    # A helper only downloads the frames it claims and sends each resulting JPEG back.
    transfer = wire_bytes * 8 / max(1, frames) / (upload_mbps * 1e6)
    remote = helper_seconds + transfer

    def finish(n):
        return max((frames - n) * typical, n * remote)

    split = frames * typical / (remote + typical)
    fastest = min((math.floor(split), math.ceil(split)), key=finish)
    needed = min(frames, max(0, frames - math.floor(max(0, budget) / typical)))
    n = needed if finish(needed) <= budget else fastest
    result.update(
        helperFrames=n,
        transferSeconds=round(n * transfer, 2),
        combinedSeconds=round(finish(n), 2),
        helperMinutes=math.ceil(n * remote / 60),
        meetsDeadline=finish(n) <= budget,
    )
    return result
