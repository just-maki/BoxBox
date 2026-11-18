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

VERIFY BEFORE TRUSTING THE NUMBERS: the header layout below is from the F1 spec
family. Open the 2026 Season Pack spec PDF and confirm HEADER_LAYOUT matches.
If a field moved, fix it in one place and everything downstream follows.
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
from collections import defaultdict

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
# CONFIRM against the spec PDF for your format before relying on the output.
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
}

# Packet types that follow the configured send rate, one per frame. Frame-counter
# gaps mean dropped packets ONLY for these. Others are event-driven or fixed-rate
# (Session ~2/s, CarDamage 10/s, SessionHistory cycles through cars), so a gap in
# their frame counter is normal and says nothing about loss.
RATE_DRIVEN = {0, 2, 6, 7, 13}

# Kernel receive buffer. Too small and the OS silently drops packets when this
# process stalls -- which would show up as "game packet loss" when it is really
# our own receiver falling behind. 8 MB is plenty of headroom at 60 Hz.
RCVBUF_BYTES = 8 * 1024 * 1024


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

        # per packetId
        self.counts: dict[int, int] = defaultdict(int)
        self.gaps_ms: dict[int, list[float]] = defaultdict(list)
        self.first_recv: dict[int, float] = {}
        self.last_recv: dict[int, float] = {}
        self.frames: dict[int, list[int]] = defaultdict(list)

        self.t_start = time.monotonic()

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

        self.counts[packet_id] += 1
        self.frames[packet_id].append(frame_id)

        prev = self.last_recv.get(packet_id)
        if prev is not None:
            self.gaps_ms[packet_id].append((t_recv - prev) * 1000.0)
        else:
            self.first_recv[packet_id] = t_recv
        self.last_recv[packet_id] = t_recv

        if self.out:
            self.out.write(
                json.dumps(
                    {
                        "t_mono": round(t_recv - self.t_start, 6),
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


def frame_loss(frames: list[int]) -> tuple[int, int, float]:
    """
    (missing, expected, fraction) from gaps in a per-packet-type frame counter.
    Only meaningful for RATE_DRIVEN packet types.
    """
    if len(frames) < 2:
        return 0, 0, 0.0
    ordered = sorted(frames)
    span = ordered[-1] - ordered[0] + 1
    received = len(set(ordered))
    missing = max(0, span - received)
    return missing, span, (missing / span if span else 0.0)


def out_of_order(frames: list[int]) -> int:
    """Count arrivals whose frame counter went backwards -- UDP reordering."""
    return sum(1 for a, b in zip(frames, frames[1:]) if b < a)


def report(cap: HeaderCapture) -> None:
    elapsed = time.monotonic() - cap.t_start

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

    hdr = (
        f"\n{'packet':<22}{'n':>8}{'Hz':>9}{'span s':>9}"
        f"{'p50':>9}{'p95':>9}{'p99':>9}{'max':>9}"
    )
    print(hdr)
    print("-" * len(hdr.strip("\n")))

    for pid in sorted(cap.counts):
        name = PACKET_NAMES.get(pid, f"Unknown{pid}")
        n = cap.counts[pid]
        gaps = sorted(cap.gaps_ms[pid])
        # Rate over the ACTIVE window for this packet type (first to last arrival),
        # not the receiver's lifetime -- idle time before and after a session would
        # otherwise drag the figure below the real send rate.
        span = cap.last_recv.get(pid, 0.0) - cap.first_recv.get(pid, 0.0)
        rate = (n - 1) / span if span > 0 else 0.0
        print(
            f"{name + ' (' + str(pid) + ')':<22}{n:>8}{rate:>9.1f}{span:>9.1f}"
            f"{pct(gaps, 0.50):>9.1f}{pct(gaps, 0.95):>9.1f}"
            f"{pct(gaps, 0.99):>9.1f}{(gaps[-1] if gaps else float('nan')):>9.1f}"
        )

    print(
        "\ninter-arrival gaps in ms. Hz is observed over each packet type's active\n"
        "window (span), so idle time before and after a session does not skew it."
    )

    print("\nLoss and ordering (rate-driven packet types only):")
    any_rate_driven = False
    for pid in sorted(cap.counts):
        if pid not in RATE_DRIVEN:
            continue
        any_rate_driven = True
        name = PACKET_NAMES.get(pid, f"Unknown{pid}")
        missing, span, frac = frame_loss(cap.frames[pid])
        ooo = out_of_order(cap.frames[pid])
        print(
            f"  {name:<16} missing {missing:>6} / {span:<6} frames "
            f"= {frac * 100:>6.3f}%   out-of-order arrivals: {ooo}"
        )

    if not any_rate_driven:
        print("  none seen -- drive a timed lap to get Motion / LapData / CarTelemetry")
    else:
        print(
            "\n  Loss here includes anything the kernel dropped under buffer pressure,\n"
            "  not just the network. Note that in the report's limitations section."
        )


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
    args = ap.parse_args()

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

    print(f"Listening on {args.bind}:{args.port}  (SO_RCVBUF {actual_buf // 1024} KiB)")
    if args.out:
        print(f"Writing {args.out}")
    print("Drive a timed lap. Ctrl+C to stop.\n")

    try:
        while not stop["flag"]:
            if args.duration and (time.monotonic() - cap.t_start) >= args.duration:
                break
            try:
                data, _addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            cap.feed(data, time.monotonic(), time.time())
    finally:
        sock.close()
        report(cap)
        cap.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
