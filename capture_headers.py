#!/usr/bin/env python3
"""
BoxBox phase 1 — header-only UDP capture.

Measures the receive path without parsing a single telemetry field:

  REQ-CAP-001  inter-arrival gap distribution per packet type
  REQ-CAP-003  packet loss, via gaps in the game's frame counter

Every F1 packet opens with the same header, and its FIRST field is the packet
format (2024 / 2025 / 2026 ...). So this script reads the format off the wire
rather than trusting the in-game menu setting -- if you switch UDP Format, this
keeps working and the log records which format each packet actually used.

Stdlib only. Run it, drive a session, Ctrl+C for the summary.

    python capture_headers.py --out captures/run01.jsonl
    python capture_headers.py --out captures/run01.jsonl --duration 300

The 2026 header layout below has been checked against the 2026 Season Pack spec
PDF (v1.2). If a later spec moves a field, fix HEADER_LAYOUT in one place and
everything downstream follows.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import struct
import sys
import time
from array import array
from bisect import bisect_right
from collections import defaultdict, deque

# --------------------------------------------------------------------------
# Header layout, per packet format.
#
# Field order (2023 onward), little-endian:
#   uint16 packetFormat          <- offset 0, read first, decides the rest
#   uint8  gameYear
#   uint8  gameMajorVersion
#   uint8  gameMinorVersion
#   uint8  packetVersion
#   uint8  packetId
#   uint64 sessionUID
#   float  sessionTime
#   uint32 frameIdentifier
#   uint32 overallFrameIdentifier
#   uint8  playerCarIndex
#   uint8  secondaryPlayerCarIndex
#
# 2026: checked field by field against "Data Output from F1 25 2026 Season
# Pack" v1.2 -- identical order and types, little-endian, packed, 29 bytes.
# 2024 / 2025: not checked against their PDFs.
# --------------------------------------------------------------------------

_HEADER_2023PLUS = struct.Struct("<HBBBBBQfIIBB")

HEADER_LAYOUT = {
    2024: _HEADER_2023PLUS,
    2025: _HEADER_2023PLUS,
    2026: _HEADER_2023PLUS,
}

PLAUSIBLE_FORMATS = range(2020, 2031)

PACKET_NAMES = {
    0: "Motion",
    1: "Session",
    2: "LapData",
    3: "Event",
    4: "Participants",
    5: "CarSetups",
    6: "CarTelemetry",
    7: "CarStatus",
    8: "FinalClassification",
    9: "LobbyInfo",
    10: "CarDamage",
    11: "SessionHistory",
    12: "TyreSets",
    13: "MotionEx",
    14: "TimeTrial",
    15: "LapPositions",
    16: "CarTelemetry2",  # new in the 2026 format
}

# Packet types that follow the configured send rate, one per frame. Frame-counter
# gaps mean dropped packets ONLY for these. Others are event-driven or fixed-rate
# (Session ~2/s, CarDamage 10/s, SessionHistory cycles through cars), so a gap in
# their frame counter is normal and says nothing about loss.
RATE_DRIVEN = {0, 2, 6, 7, 13, 16}

# Kernel receive buffer. Too small and the OS silently drops packets when this
# process stalls -- which would show up as "game packet loss" when it is really
# our own receiver falling behind. 8 MB is plenty of headroom at 60 Hz.
RCVBUF_BYTES = 8 * 1024 * 1024

# Receive timestamps use time.perf_counter(), NOT time.monotonic(). On Windows
# with Python < 3.13, monotonic() is GetTickCount64 and only ticks every
# 15.625 ms -- the first real capture's gaps came out as 0/15/16/31/32 ms, all
# clock quantisation. perf_counter() is QueryPerformanceCounter (0.1 us) and is
# also monotonic. The clock's name and resolution are printed at startup.


# Game-clock discontinuity detection (pauses, flashbacks, restarts).
#
# While the game runs, (receive time - session_time) is roughly constant. Receive
# delays can only ever push it UP, briefly, and it comes back down once the
# backlog drains. A pause, restart or flashback instead shifts it for good. So we
# compare the minimum offset over the second before a pair of packets with the
# minimum over the second after it -- a min filter, as in NTP, because delay
# noise is one-sided. Both the step between the pair and the before/after level
# change must exceed the threshold, so a delayed burst (step yes, level no) is
# NOT mistaken for a pause and its gap stays in the statistics.
DISC_WINDOW_S = 1.0
DISC_THRESHOLD_S = 0.05

# A frame-number skip counts as a lost packet only when the game-clock interval
# across it looks like a real drop. A dropped packet makes that interval the sum
# of two consecutive normal intervals, so the threshold is a low percentile of
# that two-interval distribution, measured from the same capture for the same
# packet type. Below it, the game advanced frame_id without sending.
#
# First real capture (Motion, 60 Hz setting, ~535 s): one interval p50 16.1 ms;
# two intervals p1 26.1 ms; all 319 skips 16.3-24.9 ms. No overlap, so every
# skip was game-side and loss was 0. Cost of the rule: ~1% of real drops (those
# whose two intervals sum below p1) would be called skips.
LOSS_TWO_INTERVAL_PCTL = 0.01
MIN_NORMAL_STEPS = 100


class HeaderCapture:
    def __init__(self, out_path: str | None):
        self.out = None
        if out_path:
            parent = os.path.dirname(os.path.abspath(out_path))
            os.makedirs(parent, exist_ok=True)
            self.out = open(out_path, "w", buffering=1 << 16)

        self.total = 0
        self.undersized = 0
        self.unknown_format = 0
        self.formats_seen: set[int] = set()

        # every valid packet, in arrival order -- for discontinuity detection
        self.g_t = array("d")  # receive time, s since start
        self.g_st = array("d")  # header session_time, s
        self.g_uid = array("Q")  # header session_uid

        # per packetId, in arrival order
        self.counts: dict[int, int] = defaultdict(int)
        self.t: dict[int, array] = defaultdict(lambda: array("d"))
        self.st: dict[int, array] = defaultdict(lambda: array("d"))
        self.fr: dict[int, array] = defaultdict(lambda: array("q"))
        self.uid: dict[int, array] = defaultdict(lambda: array("Q"))

        self.t_start = time.perf_counter()
        self.elapsed: float | None = None  # set by replay; live uses the clock

    def record(
        self, packet_id: int, session_uid: int, session_time: float, frame_id: int, t_rel: float
    ) -> None:
        self.counts[packet_id] += 1
        self.g_t.append(t_rel)
        self.g_st.append(session_time)
        self.g_uid.append(session_uid)
        self.t[packet_id].append(t_rel)
        self.st[packet_id].append(session_time)
        self.fr[packet_id].append(frame_id)
        self.uid[packet_id].append(session_uid)

    def feed(self, data: bytes, t_recv: float, t_wall: float) -> None:
        self.total += 1

        if len(data) < 2:
            self.undersized += 1
            return

        fmt_year = struct.unpack_from("<H", data, 0)[0]
        if fmt_year not in PLAUSIBLE_FORMATS:
            self.unknown_format += 1
            return

        self.formats_seen.add(fmt_year)
        layout = HEADER_LAYOUT.get(fmt_year, _HEADER_2023PLUS)

        if len(data) < layout.size:
            self.undersized += 1
            return

        (
            packet_format,
            game_year,
            game_major,
            game_minor,
            packet_version,
            packet_id,
            session_uid,
            session_time,
            frame_id,
            overall_frame_id,
            player_idx,
            secondary_idx,
        ) = layout.unpack_from(data, 0)

        t_rel = t_recv - self.t_start
        self.record(packet_id, session_uid, session_time, frame_id, t_rel)

        if self.out:
            self.out.write(
                json.dumps(
                    {
                        # field name kept for file compatibility; the source is
                        # perf_counter, which is monotonic
                        "t_mono": round(t_rel, 6),
                        "t_wall": round(t_wall, 6),
                        "bytes": len(data),
                        "packet_format": packet_format,
                        "game_year": game_year,
                        "game_version": f"{game_major}.{game_minor}",
                        "packet_version": packet_version,
                        "packet_id": packet_id,
                        "packet_name": PACKET_NAMES.get(packet_id, f"Unknown{packet_id}"),
                        "session_uid": session_uid,
                        "session_time": round(session_time, 4),
                        "frame_id": frame_id,
                        "overall_frame_id": overall_frame_id,
                        "player_car_index": player_idx,
                        "secondary_player_car_index": secondary_idx,
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )

    def close(self) -> None:
        if self.out:
            self.out.close()


def pct(sorted_vals: list[float], q: float) -> float:
    """Nearest-rank percentile. q in [0, 1]."""
    if not sorted_vals:
        return float("nan")
    k = max(0, min(len(sorted_vals) - 1, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[k]


def _window_min(t: array, v: list[float], window: float, forward: bool) -> list[float]:
    """
    Sliding minimum of v. Backward: over t in (t_i - window, t_i].
    Forward: over t in [t_i, t_i + window). Monotonic deque, O(n).
    """
    n = len(v)
    out = [0.0] * n
    dq: deque[int] = deque()
    order = range(n - 1, -1, -1) if forward else range(n)
    for i in order:
        while dq and v[dq[-1]] >= v[i]:
            dq.pop()
        dq.append(i)
        while abs(t[dq[0]] - t[i]) >= window:
            dq.popleft()
        out[i] = v[dq[0]]
    return out


def find_discontinuities(
    t: array, st: array, uid: array
) -> list[tuple[float, float, float, str, float, float]]:
    """
    (t_before, t_after, step_s, reason, off_before, off_after) for each break in
    the game clock. t_before / t_after are the receive times of the packets
    either side of it; off_* are (receive time - session_time) for those packets.
    """
    n = len(t)
    if n < 2:
        return []
    off = [t[i] - st[i] for i in range(n)]
    before = _window_min(t, off, DISC_WINDOW_S, forward=False)
    after = _window_min(t, off, DISC_WINDOW_S, forward=True)
    out = []
    for i in range(n - 1):
        if uid[i + 1] != uid[i]:
            out.append((t[i], t[i + 1], t[i + 1] - t[i], "new session", off[i], off[i + 1]))
            continue
        jump = off[i + 1] - off[i]
        level = after[i + 1] - before[i]
        if abs(jump) > DISC_THRESHOLD_S and abs(level) > DISC_THRESHOLD_S and (jump > 0) == (level > 0):
            reason = (
                "game clock stopped or went back (pause, flashback, restart)"
                if level > 0
                else "game clock jumped forward"
            )
            out.append((t[i], t[i + 1], level, reason, off[i], off[i + 1]))
    return out


def crosses(disc_after: list[float], t_a: float, t_b: float) -> bool:
    """True if any discontinuity completes inside the interval (t_a, t_b]."""
    k = bisect_right(disc_after, t_a)
    return k < len(disc_after) and disc_after[k] <= t_b


def gap_stats(cap: HeaderCapture, pid: int, disc_after: list[float]) -> tuple[list[float], int]:
    """(sorted gaps in ms that do not span a discontinuity, number excluded)."""
    t = cap.t[pid]
    gaps, excluded = [], 0
    for a, b in zip(t, t[1:]):
        if crosses(disc_after, a, b):
            excluded += 1
        else:
            gaps.append((b - a) * 1000.0)
    gaps.sort()
    return gaps, excluded


def loss_stats(cap: HeaderCapture, pid: int, disc_after: list[float]) -> dict:
    """
    Classify frame-number skips for one rate-driven packet type as lost packets
    or game-side skips, using the game clock across each skip.
    """
    t, st, fr, uid = cap.t[pid], cap.st[pid], cap.fr[pid], cap.uid[pid]
    # (frame step, game-clock interval) per consecutive pair; None if the pair
    # spans a new session or a game-clock discontinuity
    steps = [
        (fr[i + 1] - fr[i], st[i + 1] - st[i])
        if uid[i + 1] == uid[i] and not crosses(disc_after, t[i], t[i + 1])
        else None
        for i in range(len(t) - 1)
    ]
    pairs = [p for p in steps if p is not None]

    def normal_step(p) -> bool:
        return p is not None and p[0] == 1 and p[1] > 0

    one = sorted(p[1] for p in pairs if normal_step(p))
    two = sorted(
        a[1] + b[1] for a, b in zip(steps, steps[1:]) if normal_step(a) and normal_step(b)
    )
    res = {
        "n": cap.counts[pid],
        "skipped_frames": sum(s - 1 for s, _ in pairs if s >= 2),
        "backwards": sum(1 for s, _ in pairs if s <= 0),
        "lost": None,
        "median_ms": None,
        "threshold_ms": None,
    }
    if len(two) < MIN_NORMAL_STEPS:
        return res  # not enough normal steps to set a threshold
    med = pct(one, 0.5)
    thr = pct(two, LOSS_TWO_INTERVAL_PCTL)
    lost = 0
    for s, d in pairs:
        if s >= 2 and d >= thr:
            lost += min(s - 1, max(1, round(d / med) - 1))
    res.update(lost=lost, median_ms=med * 1000, threshold_ms=thr * 1000)
    return res


def report(cap: HeaderCapture) -> None:
    elapsed = cap.elapsed if cap.elapsed is not None else time.perf_counter() - cap.t_start

    print(f"\n{'=' * 78}")
    print(f"BoxBox header capture -- {elapsed:.1f}s, {cap.total} datagrams")
    if cap.formats_seen:
        print(f"packet format(s) seen on the wire: {sorted(cap.formats_seen)}")
    if cap.undersized:
        print(f"WARNING  {cap.undersized} datagram(s) too short to hold a header")
    if cap.unknown_format:
        print(f"WARNING  {cap.unknown_format} datagram(s) with implausible format field")
        print("         -- wrong port, or something else is sending to it")
    print("=" * 78)

    if not cap.counts:
        print("\nNo valid packets. Checklist:")
        print("  - UDP Telemetry       On")
        print("  - UDP Port            matches --port")
        print("  - UDP IP Address      127.0.0.1 if the game is on this machine")
        print("  - UDP Broadcast Mode  Off")
        print("  - you are actually driving (some packets only flow on a timed lap)")
        return

    disc = find_discontinuities(cap.g_t, cap.g_st, cap.g_uid)
    disc_after = [d[1] for d in disc]

    # Group breaks less than a window apart into episodes for display. A game
    # clock running faster than real time (e.g. skipping to the end of a
    # session) shows up as one long episode of many small forward breaks.
    # Net shift = offset just after the episode minus offset just before it.
    # Summing per-break steps would double-count when packets are sparse and
    # the detection windows overlap.
    episodes: list[list] = []  # [start, end, breaks, off_before, off_after, reasons]
    for t_b, t_a, _step, reason, off_b, off_a in disc:
        if episodes and t_b - episodes[-1][1] < DISC_WINDOW_S:
            ep = episodes[-1]
            ep[1] = t_a
            ep[2] += 1
            ep[4] = off_a
            ep[5].add(reason)
        else:
            episodes.append([t_b, t_a, 1, off_b, off_a, {reason}])

    print(
        f"\nGame-clock discontinuities: {len(disc)} breaks in {len(episodes)} episode(s).\n"
        "Gaps and frame steps spanning a break are excluded from every figure below."
    )
    for start, end, count, off_b, off_a, reasons in episodes[:12]:
        print(
            f"  {start:>8.2f}-{end:<8.2f} s  {count:>4} break(s)  net {off_a - off_b:>+8.2f} s  "
            f"{'; '.join(sorted(reasons))}"
        )
    print("  net = how far the game clock fell behind (+) or jumped ahead of (-) real time")
    if len(episodes) > 12:
        print(f"  ... and {len(episodes) - 12} more")

    hdr = (
        f"\n{'packet':<22}{'n':>8}{'Hz':>8}{'active s':>10}"
        f"{'p50':>8}{'p95':>8}{'p99':>8}{'max':>8}{'excl':>6}"
    )
    print(hdr)
    print("-" * len(hdr.strip("\n")))

    for pid in sorted(cap.counts):
        name = PACKET_NAMES.get(pid, f"Unknown{pid}")
        gaps, excluded = gap_stats(cap, pid, disc_after)
        # Rate over the gaps that remain, so pauses and idle time before and
        # after a session do not drag it below the real send rate.
        active = sum(gaps) / 1000.0
        rate = len(gaps) / active if active > 0 else 0.0
        print(
            f"{name + ' (' + str(pid) + ')':<22}{cap.counts[pid]:>8}{rate:>8.2f}{active:>10.1f}"
            f"{pct(gaps, 0.50):>8.1f}{pct(gaps, 0.95):>8.1f}"
            f"{pct(gaps, 0.99):>8.1f}{(gaps[-1] if gaps else float('nan')):>8.1f}{excluded:>6}"
        )

    print(
        "\ninter-arrival gaps in ms on the receive clock. 'excl' = gaps dropped for\n"
        "spanning a game-clock discontinuity. Hz and active s use the remaining gaps."
    )

    rd = [pid for pid in sorted(cap.counts) if pid in RATE_DRIVEN]
    print("\nLoss (rate-driven packet types only):")
    if not rd:
        print("  none seen -- drive a timed lap to get Motion / LapData / CarTelemetry")
        return

    print(
        f"  {'packet':<16}{'n':>8}{'lost':>7}{'loss %':>9}"
        f"{'skips':>8}{'upper %':>9}{'backwd':>8}{'thresh ms':>11}"
    )
    for pid in rd:
        name = PACKET_NAMES.get(pid, f"Unknown{pid}")
        r = loss_stats(cap, pid, disc_after)
        n, skips = r["n"], r["skipped_frames"]
        upper = skips / (n + skips) * 100 if n + skips else 0.0
        if r["lost"] is None:
            lost_s, pct_s, thr_s = "?", "?", "n/a"
        else:
            lost_s = str(r["lost"])
            pct_s = f"{r['lost'] / (n + r['lost']) * 100:.3f}"
            thr_s = f"{r['threshold_ms']:.1f}"
        print(
            f"  {name:<16}{n:>8}{lost_s:>7}{pct_s:>9}"
            f"{skips:>8}{upper:>9.3f}{r['backwards']:>8}{thr_s:>11}"
        )

    print(
        f"\n  skips    = frame numbers missing from the sequence.\n"
        f"  lost     = skips where the game clock ALSO shows a missing interval: the\n"
        f"             interval across the skip is at least thresh, the p{LOSS_TWO_INTERVAL_PCTL * 100:g} of\n"
        f"             two consecutive normal intervals (what a real drop looks like).\n"
        f"             The rest are the game advancing frame_id without sending.\n"
        f"             By construction ~{LOSS_TWO_INTERVAL_PCTL * 100:g}% of real drops fall below thresh.\n"
        f"  upper %  = loss if every skip were a real drop -- the bound, not the estimate.\n"
        f"  backwd   = frame number went backwards without a clock discontinuity\n"
        f"             (UDP reordering).\n"
        f"  Loss includes anything the kernel dropped under buffer pressure, not just\n"
        f"  the network. Note that in the report's limitations section."
    )


def replay(path: str) -> int:
    """Rebuild the report from a capture's JSONL. Receive gaps are only as good as
    the clock that recorded them -- captures made before the switch to
    perf_counter carry 15.625 ms quantisation on Windows."""
    cap = HeaderCapture(None)
    last_t = 0.0
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            cap.total += 1
            cap.formats_seen.add(r["packet_format"])
            cap.record(r["packet_id"], r["session_uid"], r["session_time"], r["frame_id"], r["t_mono"])
            last_t = r["t_mono"]
    cap.elapsed = last_t
    print(f"Replaying {path}")
    report(cap)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="BoxBox phase 1 header-only UDP capture")
    ap.add_argument("--port", type=int, default=20777, help="UDP port (default 20777)")
    ap.add_argument(
        "--bind",
        default="0.0.0.0",
        help="bind address; 0.0.0.0 also accepts console/LAN senders (default 0.0.0.0)",
    )
    ap.add_argument("--out", default=None, help="JSONL output path")
    ap.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    ap.add_argument(
        "--replay",
        default=None,
        metavar="JSONL",
        help="re-run the report on a saved capture instead of listening",
    )
    args = ap.parse_args()

    if args.replay:
        return replay(args.replay)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF_BYTES)
    except OSError:
        pass
    actual_buf = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)

    try:
        sock.bind((args.bind, args.port))
    except OSError as e:
        print(f"Cannot bind {args.bind}:{args.port} -- {e}", file=sys.stderr)
        print("Another telemetry app may already hold the port.", file=sys.stderr)
        return 1

    sock.settimeout(0.5)

    cap = HeaderCapture(args.out)
    stop = {"flag": False}

    def on_signal(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    clk = time.get_clock_info("perf_counter")
    print(f"Listening on {args.bind}:{args.port}  (SO_RCVBUF {actual_buf // 1024} KiB)")
    print(f"Receive clock: {clk.implementation}, resolution {clk.resolution * 1e6:g} us")
    if args.out:
        print(f"Writing {args.out}")
    print("Drive a timed lap. Ctrl+C to stop.\n")

    try:
        while not stop["flag"]:
            if args.duration and (time.perf_counter() - cap.t_start) >= args.duration:
                break
            try:
                data, _addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            cap.feed(data, time.perf_counter(), time.time())
    finally:
        sock.close()
        report(cap)
        cap.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
