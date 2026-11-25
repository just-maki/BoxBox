# BoxBox

A telemetry capture and validation pipeline for **UDP telemetry output from the F1 25
simulator (2026 Season Pack, UDP format `2026`)**. Every figure it produces is measured
against stated requirements, with the method and data source attached. Motorsport is the
data domain. Structurally, this is test-and-evaluation data infrastructure: capture a
stream, check its integrity, and measure everything built on top of it.

> **Status:** Phase 1 (capture) is written and has been run against live game sessions.
> No measurement report has been committed yet, so this README quotes no results. Phases 2
> to 10 are planned and not built. See [Roadmap](#roadmap).

---

## What exists today

| File | What it does |
|---|---|
| `capture_headers.py` | Listens for the game's UDP packets, parses the 29-byte packet header and writes one JSON line per packet. When the capture stops, prints a report covering per-type rate, inter-arrival gaps, packet loss and game-clock discontinuities. Can re-run that report on a saved capture. |
| `live_view.py` | Shows the same figures live while a capture runs, over a sliding window. Runs as a separate process (see below). |
| `docs/PLAN.md` | The build plan: phases, requirements, the margin definition and the claims policy. |

Python 3 standard library only (developed on 3.11). No dependencies to install.

The data source is simulator output. No vehicle hardware is involved, and none has been
used.

---

## Quick start

**1. Turn on telemetry in the game:** Settings → Telemetry Settings.

| Setting | Value |
|---|---|
| UDP Telemetry | On |
| UDP Broadcast Mode | Off |
| UDP IP Address | `127.0.0.1` (game on the same machine) |
| UDP Port | `20777` |
| UDP Send Rate | 10 / 20 / 30 / 60 Hz |
| UDP Format | `2026` |

**2. Capture a session**, with the live view in a second window:

```powershell
python capture_headers.py --out captures/run01.jsonl --live
```

Drive, then press Ctrl+C in the capture window. The full report prints there.

**Other options**

```powershell
python capture_headers.py --out captures/run01.jsonl --duration 300   # stop after 5 min
python capture_headers.py --replay captures/run01.jsonl               # re-run the report
python live_view.py captures/run01.jsonl                              # open the live view by hand
python capture_headers.py --port 20778                                # non-default port
```

`--live` opens a new window on Windows. On other platforms it prints the `live_view.py`
command to run in a second terminal.

---

## How the measurements work

**Two clocks per packet.** Each packet gets a receive timestamp when it arrives and also
carries the game's own `session_time` from its header. Comparing the two is what lets the
pipeline tell transport effects apart from game behaviour.

**A high-resolution receive clock.** Timestamps use `time.perf_counter()`. On Windows with
Python below 3.13, `time.monotonic()` only ticks every 15.625 ms, which turns inter-arrival
gaps into rounding artefacts. The clock in use and its resolution are printed at startup.

**Pauses are excluded, not hidden.** While the game runs, the receive time minus the game
time stays roughly constant. A network delay pushes it up briefly. A pause, flashback,
restart or new session shifts it for good. These game-clock breaks are detected with a
one-sided minimum filter, listed in the report, and gaps that span one are left out of the
statistics.

**A frame-number skip is not automatically loss.** The game sometimes advances its frame
counter without sending a packet. A skip counts as a lost packet only when the game clock
also shows a missing interval, meaning the interval across the skip looks like two normal
intervals. The threshold is derived from each capture's own data. The report also gives
the upper bound that treats every skip as a drop.

**Loss includes this machine.** Anything the OS drops under receive-buffer pressure counts
as loss. This is a limitation of the method, and reports state it.

**The live view can't affect the measurement.** The capture loop is the thing being
measured, so the live view is a separate process that reads the capture file. It uses the
same functions as the end-of-run report. It trails the stream by however long the
capture's 64 KiB write buffer takes to fill, and its newest second is provisional.

---

## Roadmap

The build order puts the measurement rig first, so requirement limits can come from
measurement rather than guesses. The anomaly detector is what gets measured, so it comes
late.

| Phase | Deliverable | Status |
|---|---|---|
| 1 | UDP capture with receive-path measurement, plus live view | Written. Rate sweep (10/20/30/60 Hz) in progress |
| 2 | Lap timing, scored against the simulator's own lap times | Planned |
| 3 | Fault injection over recorded sessions, with truth records | Planned |
| 4 | `requirements.yaml` and an evaluator with signed, normalised margin | Planned |
| 5 | HTML test report with provenance and limitations | Planned |
| 6 | Run registry and run-to-run `compare` | Planned |
| 7 | Anomaly detector, scored against injected faults | Planned |
| 8 | Technique classifier | Planned. Classes not yet defined |
| 9 | Real OBD-II data as a second source | Planned. No hardware owned |
| 10 | Review GUI | Planned |

Details, requirement drafts and the reasoning behind each limit are in
[`docs/PLAN.md`](docs/PLAN.md).

---

## Claims policy

- Figures appear here only after a committed report has measured them, and each one names
  its data source, e.g. "… in simulator telemetry".
- Unbuilt components are described as planned.
- Code that hasn't run against real hardware is described as written, never as used.

---

## The UDP specification

The packet layouts come from EA's *Data Output from F1 25: 2026 Season Pack* specification
(checked against v1.2). It is published by EA on the F1 game forums and is not
redistributed in this repository.

F1 is a trademark of Formula One Licensing B.V. This project is not affiliated with or
endorsed by EA, Codemasters or Formula One.

## License

MIT. See [LICENSE](LICENSE).
