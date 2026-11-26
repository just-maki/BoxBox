#!/usr/bin/env python3
"""
BoxBox live view -- watches a capture while it is being written.

This is a SEPARATE process from capture_headers.py on purpose. The capture loop
is the thing REQ-CAP-001 measures; drawing a display inside it would add the
display's own timing jitter to the figures being reported. This process only
reads the JSONL the capture writes, so the capture behaves exactly as it does
without a viewer.

    python capture_headers.py --out captures/run.jsonl --live    (opens this)
    python live_view.py captures/run.jsonl                       (or by hand)

Figures come from the same functions as the end-of-run report, applied to a
sliding window, so live and final numbers agree by construction. Two caveats,
also shown on screen:

  - The newest ~1 s is provisional: pause detection compares the second before
    each packet with the second after it.
  - The capture buffers its writes (64 KiB) to keep the receive loop cheap, so
    this view trails the wire by however long 64 KiB takes to fill. Measured on
    the first real capture: ~118 KiB/s at the 60 Hz setting, so ~0.5 s behind.
    Lower send rates write less, so expect ~2 s behind at 10 Hz.

Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import deque

import capture_headers as ch

STALE_S = 2.0  # no new data for this long -> "NO DATA"
FROZEN_S = 0.5  # game clock flat over this much receive time -> "STOPPED"


def build(rows: deque) -> ch.HeaderCapture:
    cap = ch.HeaderCapture(None)
    for t, pid, uid, st, ofr in rows:
        cap.record(pid, uid, st, ofr, t)
    return cap


def status(rows: deque, stale: float) -> str:
    if not rows:
        return "WAITING  no packets yet -- is UDP Telemetry on?"
    if stale > STALE_S:
        return f"NO DATA  nothing new for {stale:.0f} s (capture stopped, or game not sending)"
    t_last, st_last = rows[-1][0], rows[-1][3]
    recent = [r[3] for r in rows if r[0] >= t_last - FROZEN_S]
    if t_last - rows[0][0] >= FROZEN_S and max(recent) - min(recent) < 0.01:
        return f"STOPPED  game clock not advancing (paused, menus, garage)  session_time {st_last:.1f}"
    return f"RUNNING  session_time {st_last:.1f} s"


def render(path: str, rows: deque, window: float, totals: dict, stale: float) -> str:
    out = [
        f"BoxBox live view   {os.path.basename(path)}",
        f"captured {totals['packets']} packets over {totals['t_last']:.0f} s"
        + (f"   ({totals['bad']} unreadable lines)" if totals["bad"] else ""),
        status(rows, stale),
        "",
    ]
    if not rows:
        return "\n".join(out)

    cap = build(rows)
    disc = ch.find_discontinuities(cap.g_t, cap.g_st, cap.g_uid)
    disc_after = [d[1] for d in disc]

    out.append(
        f"last {window:g} s   game-clock breaks in window: {len(disc)}"
        "   (gaps spanning one are excluded)"
    )
    out.append("")
    out.append(
        f"{'packet':<22}{'Hz':>7}{'p50':>8}{'p95':>8}{'max':>8}"
        f"{'lost':>6}{'absent':>8}{'excl':>6}"
    )
    out.append("-" * 73)
    fl = ch.frame_loss(cap)
    for pid in sorted(cap.counts):
        name = ch.PACKET_NAMES.get(pid, f"Unknown{pid}")
        gaps, excluded = ch.gap_stats(cap, pid, disc_after)
        active = sum(gaps) / 1000.0
        rate = len(gaps) / active if active > 0 else 0.0
        r = fl["types"].get(pid)
        lost = str(r["lost"]) if r else ""
        absent = str(r["absent"]) if r else ""
        out.append(
            f"{name + ' (' + str(pid) + ')':<22}{rate:>7.2f}"
            f"{ch.pct(gaps, 0.50):>8.1f}{ch.pct(gaps, 0.95):>8.1f}"
            f"{(gaps[-1] if gaps else float('nan')):>8.1f}"
            f"{lost:>6}{absent:>8}{excluded:>6}"
        )
    out += [
        "",
        f"frames checked: {fl['checked']}   frame numbers with no rate-driven packet: "
        f"{fl['whole_missing']} (not counted as loss)",
        "",
        "gaps in ms on the receive clock. lost = missing from a frame in which other",
        "rate-driven types arrived. absent = type not sent for a stretch.",
        "Newest ~1 s is provisional. Final figures: the report printed when capture ends.",
        "Ctrl+C to close this window. It does not stop the capture.",
    ]
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description="BoxBox live view of a capture in progress")
    ap.add_argument("path", help="JSONL file the capture is writing")
    ap.add_argument("--window", type=float, default=15.0, help="sliding window, s (default 15)")
    ap.add_argument("--interval", type=float, default=1.0, help="redraw every N s (default 1)")
    args = ap.parse_args()

    if os.name == "nt":
        os.system("")  # turns on ANSI escape handling in the Windows console

    print(f"Waiting for {args.path} ...")
    while not os.path.exists(args.path):
        time.sleep(0.25)

    f = open(args.path, "r")
    buf = ""
    rows: deque = deque()
    totals = {"packets": 0, "bad": 0, "t_last": 0.0}
    last_growth = time.monotonic()
    next_draw = 0.0

    try:
        while True:
            # a new capture to the same path truncates the file -- start over
            if os.path.getsize(args.path) < f.tell():
                f.close()
                f = open(args.path, "r")
                buf, rows = "", deque()
                totals = {"packets": 0, "bad": 0, "t_last": 0.0}

            chunk = f.read()
            if chunk:
                last_growth = time.monotonic()
                buf += chunk
                *complete, buf = buf.split("\n")
                for line in complete:
                    if not line:
                        continue
                    try:
                        r = json.loads(line)
                        row = (r["t_mono"], r["packet_id"], r["session_uid"], r["session_time"], r["overall_frame_id"])
                    except (ValueError, KeyError):
                        totals["bad"] += 1
                        continue
                    rows.append(row)
                    totals["packets"] += 1
                    totals["t_last"] = row[0]
                while rows and rows[0][0] < totals["t_last"] - args.window:
                    rows.popleft()

            now = time.monotonic()
            if now >= next_draw:
                screen = render(args.path, rows, args.window, totals, now - last_growth)
                sys.stdout.write("\x1b[H\x1b[J" + screen + "\n")
                sys.stdout.flush()
                next_draw = now + args.interval
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        f.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
