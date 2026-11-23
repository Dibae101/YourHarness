# Review Studio

Review Studio turns image sequences and Blender motion previews into silent MP4s. An invited person can lend browser workers to one review for a chosen time, then reclaim their device. The owner and helpers send frames through the coordinator. The coordinator must stay online to save results and assemble the video.

## Start the app

Use the current Python application from this repository, or a desktop build made from this revision. From the repository root on macOS or Linux:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[qr]'
.venv/bin/harness serve
```

On Windows use Python 3.10 or newer and run the equivalent commands with `.venv\Scripts\python.exe` and `.venv\Scripts\harness.exe`. Open the printed address, join as an owner, and select **Review Studio** from the dock or launcher. The studio is also at `/reviews.html`. Choose **Try the demo** to produce a 72-frame turntable without installing Blender. Use **Pool workspace** to return to the desktop.

## Choose a source

| Source | What to select | What a helper receives |
| --- | --- | --- |
| Image sequence | JPEG or PNG files, ordered by filename with numeric sorting. Choose the playback frame rate. | Assigned images normalized to JPEG. The importer applies EXIF orientation and strips metadata. People processing an image can see it. |
| Blender motion review | Download the exporter in **New review → Blender motion review**. In Blender, install the Python add-on from disk and enable it. Use **File → Export → YourHarness motion review** and upload the resulting `.yhreview` file. | Projected mesh triangles, depth and flat colors for assigned frames. Camera motion and visible geometry are shareable information. The original Blender file stays on the artist's computer. |
| Turntable demo | Select the built-in source. | Generated projected geometry. |

The Blender adapter is a camera and animation blocking preview. It uses evaluated mesh instances, diffuse display colors and simple lighting. Final Cycles materials, textures, audio, compositing, volumetrics and near-plane clipping effects need a different render pipeline. A triangle crossing the near plane is omitted in this version. Select 360, 640 or 960 pixels for the longest edge. Inputs are limited to 128 MiB, 1,800 frames, 12 megapixels per source image and 24,000 visible triangles per exported frame.

## From import to delivery

1. **Measure.** Choose the quality, name, and deadline. Keep the studio tab visible. It renders the first, middle and last frames and keeps the results.
2. **Plan.** Inspect the measured local range. To compare a helper, select earlier measurements for the same source type and preview size, or supply a frame time and connection speed. Those are user-supplied assumptions. The comparison includes the estimated transfer of assigned inputs and results. Assembly history is shown when available. A deadline or speedup is never guaranteed.
3. **Render.** Select **Start full review**. Review the source list and allowance before making a one-use invitation. Send the private link to a teammate. They see the scope and choose a name, duration, concurrency and render-buffer budget. The browser limits simultaneous workers and ends the loan when time expires, when the helper stops or when their tab becomes hidden. Browser and OS memory overhead are outside the buffer budget.
4. **Deliver.** Watch per-device contributions. Pause or cancel the review, or revoke individual invitations. The coordinator keeps accepted frames, fences expired attempts and reassigns unfinished frames. When every frame is present, FFmpeg builds a silent H.264 MP4 for playback or download. A failed assembly can be retried without re-rendering.

One open owner tab contributes one worker to the selected review. Other signed-in owners can open the same review to help. Accepted helpers can keep a started review moving while the creator's tab is closed. A hidden or closed browser cannot provide continuous compute. An abruptly disconnected worker's lease expires in 90 seconds. Job state survives a coordinator restart. A completed or cancelled review can be deleted from the detail page.

## Across homes

Use a coordinator address that both people can reach, such as a private HTTPS address shared through Tailscale. Set `--public-url` to that address and `HARNESS_ONLINE=1` for an internet-reachable pool. Copy a **review invitation** to lend for one job. A pool owner link grants broader privileges. A helper needs a modern browser with workers, Canvas and OffscreenCanvas. [Connection options](security.md)

## Verification and further development

Run `python -m pytest` for server tests. For the two-browser workflow, install the project in a Python environment, run `npm ci` and `npx playwright install chromium`, then `node tests/web/reviews_e2e.mjs`. Set `YH_PYTHON` to the environment's Python, `YH_CHROME` to use an installed Chromium/Chrome binary, or `YH_BLENDER_PACKAGE` to include an exported scene. The test starts a private temporary coordinator.

The wider product ideas, proposed paid pilot and physical two-home study are in the [concept and field trial](job-lending-concept.md). General-purpose remote RAM, encrypted guest disk lending and final-quality GPU rendering are outside this release.
