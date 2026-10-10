<<<<<<< HEAD
# YourHarness-
=======
# YourHarness

Pool the processors of every device on your home network into one workbench you open in a browser. Your Mac, Windows PC, iPhone, iPad, or Android phone each open the same page, join with a PIN, and from then on every job you start is split into pieces that all of them work on at once.

Nothing to install on the devices. One machine runs a small Node server; everything else is a browser tab.

## Quick start

On the machine that will host the pool (any Mac, Windows, or Linux machine with Node 18 or newer):

```bash
git clone https://github.com/Dibae101/YourHarness-.git
cd YourHarness-
npm install
npm start
```

The terminal prints a link with the PIN built in, a QR code, and the PIN on its own:

```
Open this on every device you want in the pool:
  http://192.168.1.20:8420/?pin=482913

PIN: 482913
```

Open that link on every device (scan the QR code with a phone camera). Each device shows up as a colored wire in the Devices list. Pick a job, press **Run on the pool**, and watch the pieces get colored by whichever device computed them.

All devices need to be on the same network as the host.

## What you can run

| Job | What each device does | Good for |
| --- | --- | --- |
| Render a fractal | Computes strips of a Mandelbrot image. Each strip is marked with the color of the device that drew it. | Seeing the split happen live |
| Count primes | Sieves its own slice of the number range | Raw CPU comparison between devices |
| Estimate π | Throws random points at its own batch | Long, even workloads |
| Search a file | Reads its own byte range of a shared file over HTTP and counts matches | Combining shared storage with shared compute |
| Your own code | Calls your `run(chunk, total)` function for each piece it gets | Anything you can split into independent pieces |

### Writing your own job

```js
function run(chunk, total) {
  // chunk goes from 0 to total - 1. Do 1/total of the work.
  let sum = 0;
  for (let i = chunk; i < 2e9; i += total) sum += Math.sqrt(i);
  return sum; // a number, or anything JSON can hold
}
```

Pick how many pieces to split into and how to combine the results (add, largest, smallest, or keep the list). `run` can be `async`, so it can `fetch('/api/files/<name>', { headers: { 'x-harness-pin': '<pin>' } })` to read shared files.

## How it works

```
 Browser tab on each device                 Host machine (npm start)
 ┌───────────────────────────┐   WebSocket  ┌──────────────────────────────┐
 │ Portal UI                 │◄────────────►│ Device registry              │
 │ Web Workers, one per      │   pieces,    │ Scheduler (pull based)       │
 │ shared thread             │   results    │ Job planning and combining   │
 └───────────────────────────┘              │ Shared folder (HTTP, ranges) │
                                            └──────────────────────────────┘
```

- **Pull-based scheduling.** A device receives a new piece whenever one of its threads is free, so a fast PC naturally does more than a phone, and a slow device never holds up the run.
- **Dropped devices are handled.** If a phone locks or a tab closes, the server notices the missed heartbeat (about 10 to 20 seconds) and hands that device's pieces to someone else. Pieces that run far too long are reassigned after `CHUNK_TIMEOUT_S`.
- **Shared folder.** Files added from any device land in `./shared` on the host. Every device can list, download, and read byte ranges, which is how the file search splits work.
- **One run at a time.** New runs queue behind the current one.

## Honest limits

This pools *processors*, not memory. Each device works on its own pieces with its own RAM; the network is far too slow to make several devices act like one machine with one big memory. What works well is any job that splits into independent pieces. A single program that is not written that way will not get faster.

- **Phones and tablets pause background tabs.** Keep the page open and the screen on. Over HTTPS the page holds a screen wake lock for you; over plain `http://` on a LAN, browsers do not allow that.
- **Browser tabs get only part of a device's memory**, and Safari does not report memory at all.
- **Jobs must be JavaScript or WebAssembly.** A Windows `.exe` cannot run on a phone.
- **Tiny pieces are dominated by Wi-Fi latency.** Aim for pieces that take at least 100 ms each.

## Security

- Anyone with the PIN can run code on every sharing device. Keep the PIN to yourself and run this only on a network you trust.
- The PIN is random on every start unless you set `HARNESS_PIN`.
- Custom code runs inside Web Workers in each browser tab, with the same access as any web page.
- Download links carry the PIN in the URL so they work as plain links. Fine on a home network, not something to share.

For HTTPS (which also unlocks the screen wake lock and WebGPU), put the server behind something that terminates TLS, for example `tailscale serve --bg 8420`, or Caddy with its local certificate authority.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | `8420` | Port to listen on |
| `HOST` | `0.0.0.0` | Address to bind |
| `HARNESS_PIN` | random 6 digits | Fixed PIN instead of a new one each start |
| `SHARED_DIR` | `./shared` | Folder used for shared files |
| `MAX_UPLOAD_MB` | `4096` | Largest file you can add |
| `CHUNK_TIMEOUT_S` | `300` | Seconds before a stuck piece is given to another device |
| `NO_QR` | unset | Set to skip the terminal QR code |

## Project layout

```
server.js         HTTP server, WebSocket registry, scheduler, shared folder API
public/index.html Portal markup
public/style.css  Portal styles
public/app.js     Joining, the local worker pool, live dashboard, forms
public/worker.js  One background thread; runs a piece and reports back
public/jobs.js    The compute code for every job type (shared with the tests)
test/smoke.mjs    End-to-end test with simulated devices
```

## Tests

```bash
npm test
```

Starts the server, connects simulated devices that use the same compute code as the browser, runs every job type, drops a device mid-run, and checks the answers (for example, 664,579 primes below 10 million).

## Roadmap

- WebGPU kernels for devices that have it, so GPUs join the pool too
- Direct device-to-device transfers over WebRTC for jobs that move a lot of data
- LLM inference across devices by splitting model layers, building on projects such as SwarmLLM or llama.cpp RPC
- Resumable runs that survive a server restart
>>>>>>> 773c213 (YourHarness: pool home devices into one browser-based workbench)
