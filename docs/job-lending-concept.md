# YourHarness product concept

## The idea

Ask someone you trust to lend their computer **for one job, until a time they choose**. You tell YourHarness what you need finished and by when. It estimates what your devices can do alone, whether a second computer would save time after transferring the files, and how much help to ask for. Your friend sees the request, chooses their limits, and can stop at any time. You get the finished result back on your laptop.

The intended customer for the first version is a small animation team with a client review coming up, several computers in different homes, and nobody available to manage a render farm. Blender is a free tool for making 3D scenes and animations. Rendering turns a scene into the frames of a video. Independent frames give us a clear first workload and a result the customer can judge by pressing Play. Blender would be one optional integration. People using other jobs would have no reason to install it.

## What the experience could look like

These times and sizes illustrate the interaction. They are not benchmark results.

1. At 3:10, an artist opens an animation and asks for a complete review copy by 3:50, ahead of a 4:00 call. They choose the quality they are comfortable showing.
2. YourHarness times representative frames, checks compatible computers, and estimates the finish on the artist's own hardware. In this example the estimate is around 4:15. It shows a range because some frames take longer.
3. It estimates how much sooner a teammate's desktop could finish the cut, including uploading the scene and returning the frames. If that connection is too slow, it says so while the artist still has time to choose a smaller preview.
4. The artist sees exactly which project and assets the teammate would receive. If the project can be shared, the artist sends one invitation. A teammate might see *“Help Darshan render this animation? About 800 MB to download. Lend up to two workers until 3:45. Stop whenever you need your computer.”*
5. The artist's machines keep working while the teammate decides. If the teammate accepts, the app assigns just enough remaining frames to make a difference. If they leave early, it keeps finished frames, reassigns unfinished ones, and updates the estimate.
6. The artist receives a playable copy covering the entire sequence and can review timing and camera motion on a laptop or iPad. A separate final-quality render can run later. A predicted deadline is an estimate, and the app should show when it slips.

## What someone would pay for

The decision *before* inviting a colleague is part of the product. An artist can already send a message asking for help. The hard questions are whether that help will arrive in time, what the teammate will have to install or receive, and who will assemble the frames afterward. The product earns its place if it answers those questions and produces the cut while the artist is still preparing for the meeting.

Existing products already cover parts of the problem. Flamenco manages render workers, BDRO offers private Blender farms across the internet, and Blendergrid lets buyers choose a cloud rendering deadline. The hypothesis worth testing is whether a small team prefers a temporary request to someone they know when their own hardware nearly covers a deadline. Originality and demand both need evidence. [Flamenco](https://flamenco.blender.org/usage/quickstart/) · [BDRO](https://www.bdro.net/) · [Blendergrid](https://blendergrid.com/faqs)

One proposed price experiment is **$29 per team per month**, with invited helpers participating free. The price is a question for a paid pilot. The strongest competing choices are using a quick local preview, buying an occasional cloud render, or asking a colleague manually.

## What a complete first version would need

| Part | What the person should experience |
| --- | --- |
| Estimate | See a finish-time range for the current machines and the estimated gain from borrowing one more. Count preparation, file transfer, queueing, rendering, and video assembly. Label unmeasured assumptions. |
| Artist approval | Choose a review quality and see the assets and recipient before sending a shareable project. Some client projects must stay on the team's own devices. |
| Lender approval | See one named job, its approximate download, expected duration, and end time. Pick local CPU and memory limits. Pause or reclaim the device. |
| Execution | Use a narrowly scoped worker for the approved workload. A helper invitation grants access only to that job. Render inputs must be visible to the machine that processes them. |
| Recovery | Preserve accepted frames. If someone declines, disconnects, or takes the computer back, show the new estimate and continue wherever possible. |
| Delivery | Play the whole cut on the owner's device, download it, and see which machines contributed and how long the job actually took. |

## Where the wider platform fits

- **CPU and GPU work.** Split independent frames, clips, files, or test groups. Each worker must support the requested operation. GPU rendering needs a working integration and measurements on each supported configuration.
- **Memory.** Put a memory-heavy task on a machine where it fits, or divide a supported task into smaller pieces. An ordinary application on an 8 GB laptop still sees its own 8 GB. A lender's memory limit must be enforced on the lending machine.
- **Storage.** Later, let a friend opt into a fixed amount of disk for a fixed period. Encrypt files before sending them, track replicas, and show whether enough copies are online to retrieve a file. Compute consent and storage consent should be separate.
- **Phones and tablets.** Use them first to request a job, approve help, watch progress, and play the result. A visible browser tab can also contribute small supported tasks. A locked phone is a poor dependency for a deadline.
- **Other first-party jobs.** Video proxies or batch conversions could help editors. Test-suite sharding could help developers. Each integration should first establish a result that can be checked and a workload large enough to benefit from borrowing a device.

The repository's job engine provides a starting point for partitioning work, retrying it, and tracking contributions. Blender packaging, a renderer with lender-enforced limits, and finish-time estimates for real scenes are separate engineering work. [Job engine](engine.md) · [Existing implementation status](blueprint-status.md)

## Proof of concept to run later

**Question:** On a real team project, does one temporary helper get a usable cut to the artist before the review, with less effort than the team's usual workaround?

Use one shareable animation, an artist's laptop, and a teammate's computer on another home connection. Agree on a frame range, resolution, quality, and deadline in advance. Keep them the same for each run. Record whether files were cached so a warm second run does not overstate the benefit.

| Run | What to record |
| --- | --- |
| The team's usual method | Time from deciding to make a review cut until a person can play the complete sequence, hands-on setup time, and any cloud cost. |
| YourHarness on owned machines | Predicted and actual finish, time to prepare the scene, render time, and whether the cut is acceptable for the meeting. |
| YourHarness with one invited teammate | Time waiting for consent, bytes and time in transit, frames contributed, actual finish, and whether borrowing improved the outcome. |
| The teammate leaves mid-job | Whether their computer stops within the agreed limit, remaining frames finish once, the estimate updates, and the resulting video plays from beginning to end. |
| Upload would take too long | Whether the tool correctly recommends a local cut before the artist sends the full scene. |

The first technical success is a complete playable cut with every requested frame in order, at the agreed quality, and a helper who can leave without losing accepted work. Record slowdowns and missed deadlines as carefully as wins.

For the business test, recruit five teams that remember a recent late review. Ask them to bring that scene. A proposed signal to continue is three teams returning with a second real project and two willing to pay the proposed pilot price. Keep their preferred workaround and reasons for declining in the notes.

## Revision notes

| Decision to revisit | Evidence to collect |
| --- | --- |
| Are animation teams the right first customer? | Frequency of late reviews, current workarounds, and whether projects may leave the studio. |
| Is the estimate valuable enough to change a decision? | Estimated versus actual end-to-end times, and cases where staying local saved time. |
| Will a teammate lend a machine? | Acceptance rate, setup friction, and the limits people actually choose. |
| Does another workload deserve priority? | A recurring task, an identifiable buyer, and a measurable improvement on real hardware. |
