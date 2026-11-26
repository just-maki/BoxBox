# BoxBox — Build Plan (Measurement-First)

Written 2026-11-18. Supersedes `BOXBOX_HANDOFF.md` (2026-11-17).
Intended to live at `docs/PLAN.md`.

---

## 0. Status: nothing is built

- Repo `just-maki/boxbox` at commit `19141cc` (2026-06-17) contains `README.md` and
  `LICENSE`. No source code.
- No telemetry hardware owned. OBD-II and RaceBox sensors were never purchased.
- The prior handoff listed six components under "already built." None of them exist.

This is a build plan, not a change plan. The prior document was written from project
descriptions that overstated what existed; this one is written from the repo.

---

## 1. What BoxBox will be

A telemetry capture, validation and anomaly-detection pipeline, measured against stated
requirements. Motorsport is the data domain, not the identity — structurally this is the
same thing as flight test data infrastructure with a different payload.

What makes it a portfolio piece rather than a demo is that every claim has a number and a
method behind it. The gap in the original project was never capability, it was
measurement. That stays the organising idea.

---

## 2. Data sources

### 2.1 Primary — F1 25 UDP telemetry (available now, $0 additional)

Chosen over hardware for four reasons, in order of importance:

1. **It ships its own ground truth for lap timing.** The prior plan's `REQ-LAP-001` had no
   reference to measure against and might have needed one built from scratch. The game
   reports lap and sector times in milliseconds directly.
2. Already owned — no spend, no shipping, no waiting.
3. Many sessions, many tracks, repeatable conditions. Fixes the single-trace problem.
4. A documented, versioned binary interface. EA publishes the UDP specification.

**Mechanics.** UDP to `127.0.0.1:20777` by default. Send rate selectable at 10 / 20 / 30 /
60 Hz. UDP Format set to `2025` or `2026` (see open questions).

**Multi-rate is real**, which preserves the fused-stream architecture: most packets follow
the configured menu rate, while some run at a fixed rate independent of it (the damage
packet updates at 10/s). So stream-alignment measurement has a genuine subject.

| Packet | ID | Use |
|---|---|---|
| Motion | 0 | position, velocity, g-force → input to lap timing |
| Session | 1 | track, session type, weather → provenance |
| Lap Data | 2 | lap + sector times in ms, lap distance → **lap timing reference** |
| Car Telemetry | 6 | speed, throttle, brake, gear, rpm, steering → detector channels |
| Session History | 11 | per-lap times for the session → reference cross-check |
| Motion Ex | 13 | wheel-level data, suspension → additional channels |

### 2.2 Second source — real OBD-II (phase 9, ~$50–80)

What the budget buys, and what it provably cannot:

- **Adapters.** STN1110-based (OBDLink LX / MX+): $50–80, handles CAN-FD, needed for 2020+
  vehicles. Genuine ELM327 v1.5+: $10–25.
- **The rate ceiling is shared across channels, not per channel.** A cheap clone averages
  ~25 PIDs/second *total* — one channel at 25 Hz, or three at ~8 Hz, or 25 at 1 Hz. A
  genuine OBDLink manages 24–30 PIDs/s over Bluetooth and 50–60 over USB; a wired ElmScan
  claims ~130/s; basic microcontroller setups should expect 5–10/s.
- **Therefore a 25 Hz multi-channel OBD-II stream is not achievable.** Five channels at
  25 Hz needs 125 PIDs/s — the very top of the wired range, and nowhere near reachable over
  Bluetooth. The prior `REQ-CAP-001` was never physically satisfiable from OBD-II at any
  budget. This is a hard constraint, not a cost problem.
- **25 Hz position data is not purchasable at this budget at all.** RaceBox Mini and Mini S
  run $209–349. Phone GPS is ~1 Hz, far too coarse for a 50 ms lap-timing target. A u-blox
  10–25 Hz GNSS module on an ESP32 costs $30–40 but is a build project, not a purchase.

**Deferred option.** An MCP2515-based CAN shield (~$10–15) bypasses the ELM327 rate ceiling
entirely by reading raw frames. Cost is per-vehicle frame reverse-engineering. High effort,
and unusually relevant to avionics and flight-software work (bus protocols, DBC files).
Not phase 1.

**Why it is still worth doing eventually.** Sim-to-real validation. "Validated the pipeline
against simulator ground truth, then ran it on real vehicle data" is a legitimate T&E
narrative, and it makes the report's provenance field distinguish two real sources rather
than decorate one.

### 2.3 Synthetic — derived from captured sessions

Seeded from actual captured sim sessions once phase 1 exists, not from a single real pass.
Used for volume and for sweeping conditions, never for primary claims.

---

## 3. Build order

The prior plan treated the detector as built and measurement as the addition. Reality
inverts it:

| Phase | Deliverable | Depends on |
|---|---|---|
| 1 | UDP capture → timestamped records on disk | — |
| 2 | Lap timing, scored against the simulator's own lap times | 1 |
| 3 | Fault injection over recorded sessions + truth records | 1 |
| 4 | `requirements.yaml` + evaluator with margin | 2, 3 |
| 5 | Test report generator | 4 |
| 6 | Run registry + `compare` | 4 |
| 7 | Anomaly detector, scored against injected faults | 3, 4 |
| 8 | Technique classifier | 1, 4 |
| 9 | Real OBD-II as a second source | 1 |
| 10 | Review GUI | 2 |

**Why this order.** The detector is the thing being measured, so the measurement rig has to
exist first. Built this way, the limits in `requirements.yaml` are derived from first
measurements rather than guessed — which means §7's instruction to say *why* a limit was
chosen has an actual answer.

**Phase 2 is the earliest presentable result**: a measured lap-timing error distribution
against independent ground truth. If time runs short, stopping there leaves a complete,
honest, measured claim rather than a half-finished pipeline.

---

## 4. Phase 1 — Capture

Decisions, answering the prior document's open questions:

- **Storage: Parquet**, one file per session, one table per packet type. The injection pass
  and the evaluator both want columnar scans. SQLite is for the run registry, not the
  stream.
- **Two clocks, deliberately.** Record a monotonic receive timestamp per packet at arrival,
  alongside the packet header's own session time. The difference between them is what
  stream-alignment measurement is *for*.
- **Do not fuse on ingest.** Land raw per-packet-type tables and fuse in a separate step.
  Keeps capture auditable and keeps injected-fault effects attributable.

Measure from day one:

- Inter-arrival gap distribution per packet type (p50 / p95 / p99).
- **Packet loss rate against configured send rate.** UDP has no delivery guarantee and
  higher send rates are known to increase loss. Sweep 10 / 20 / 30 / 60 Hz and report the
  curve. This is a finding, not a defect — and it is a naturally occurring fault, free.

### 4.1 Live view of capture health

`live_view.py` shows capture health while a session runs — per-packet-type rate, gap
p50 / p95 / max, frame skips, confirmed drops, game-clock state — the role a quick-look
monitor plays in flight test: knowing the test is producing good data before it ends.

Rules, so the live view does not undermine what it reports:

1. **Separate process.** It reads the capture's JSONL; it never runs inside the receive
   loop, because that loop is what `REQ-CAP-001` measures.
2. **Same functions as the end-of-run report**, on a sliding window, so live and final
   figures agree by construction.
3. **Only metrics already measured offline go live.** Lap timing goes live after phase 2
   has its error distribution; anomaly alerts after phase 7 has P_d and false-alarm rate.
   Not before.
4. **Its own delay is a measured figure.** Currently it is bounded by the capture's 64 KiB
   write buffer: ~0.5 s at the 60 Hz setting, ~2 s at 10 Hz. Packet-to-screen latency as a
   distribution is not yet measured.

Driver-facing feedback (delta to best lap, braking points) is out of scope: it is a
sim-racing product, not a measurement rig.

---

## 5. Phase 2 — Lap timing and its reference

**Algorithm.** Virtual-gate line-intersection over Motion-packet positions, with linear
interpolation between samples for sub-sample resolution.

**Reference.** The Lap Data packet's last-lap and sector times in milliseconds,
cross-checked against Session History.

**Report.** Signed error distribution, p50 / p95 / p99, per track. Not a single accuracy
number — aggregate accuracy hides the failure mode.

**Secondary check.** Lap distance from the Lap Data packet gives an independent check on
gate placement, separate from the timing comparison.

---

## 6. Phase 3 — Fault injection

A separate pass over recorded Parquet, never baked into capture. The same clean session is
kept with and without each fault, so detector behaviour is attributable.

| Type | Parameters |
|---|---|
| `sensor_dropout` | channel, start, duration (samples removed) |
| `stuck_value` | channel, start, duration (frozen at last value) |
| `drift` | channel, start, duration, bias rate |
| `clock_skew` | offset in ms between two packet streams |
| `position_jump` | start, displacement magnitude |
| `packet_reorder` | start, window, shuffle depth |

`packet_reorder` is new. UDP can deliver out of order, so it is a fault class that occurs
naturally in this source and that the prior document omitted. Measure its natural rate
before injecting it.

Truth record, one per injected session:

```json
{
  "session_id": "f125_monza_tt_0042",
  "source": {
    "kind": "sim", "title": "F1 25", "udp_format": "2025",
    "send_rate_hz": 20, "track": "Monza", "session_type": "time_trial"
  },
  "clean_capture": "captures/f125_monza_tt_0042.parquet",
  "faults": [
    {"type": "sensor_dropout", "channel": "car_telemetry.engine_rpm",
     "start_s": 12.40, "duration_s": 0.80, "n_samples": 16},
    {"type": "drift", "channel": "motion.world_position_x",
     "start_s": 45.00, "duration_s": 10.00, "rate_per_s": 0.02}
  ]
}
```

---

## 7. Phase 4 — Requirements and margin

### Margin, defined

The prior document asked for "PASS/FAIL plus margin" without defining margin across mixed
limit types, which left the comparison step in §9 unable to rank anything. Define it as
signed and normalised:

- max limit: `margin = (limit − measured) / |limit|`
- min limit: `margin = (measured − limit) / |limit|`
- multi-part metric: `margin = min(part margins)` — the worst part governs
- Positive means passing. Unitless, so margins are comparable across requirements with
  different units and differenceable across runs.

That makes "moved toward failure" well-defined in phase 6.

### requirements.yaml

Every limit below is `provisional` until first measurement. Deriving limits from
measurement and then stating the reasoning is the point; inheriting them from a guess is
not.

```yaml
- id: REQ-CAP-001
  statement: >
    Capture shall record received packets with no inter-arrival gap exceeding
    LIMIT ms at a configured send rate of 20 Hz
  verification: test
  metric: max_interarrival_gap_ms
  limit: {max: 80}          # provisional — derive from the phase 1 rate sweep
  notes: >
    Measures the receive path (game scheduler, UDP transport, receiver loop),
    not sensor hardware. Must be labelled as such in any report.

- id: REQ-CAP-002
  statement: Packet streams shall remain time-aligned within LIMIT ms
  verification: test
  metric: stream_alignment_abs_ms
  limit: {p95: 20}          # provisional
  notes: >
    Measured between packet types using receive timestamps against the
    packet header's session time.

- id: REQ-CAP-003
  statement: >
    Packet loss shall not exceed LIMIT at a configured send rate of 20 Hz
  verification: test
  metric: packet_loss_fraction
  limit: {max: 0.01}        # provisional — report the full rate sweep alongside

- id: REQ-LAP-001
  statement: >
    Lap timing shall be within LIMIT ms of the simulator's reported lap time
  verification: test
  metric: lap_time_abs_error_ms
  limit: {p95: 50}          # provisional
  notes: >
    Reference is the Lap Data packet, cross-checked against Session History.

- id: REQ-ANOM-001
  statement: >
    The detector shall flag at least 90% of injected faults at no more than
    1 false alarm per lap
  verification: test
  status: not_yet_verified
  reason: Detector not implemented (phase 7).
  metric:
    p_detect: {min: 0.90}
    false_alarms_per_lap: {max: 1.0}

- id: REQ-CLS-001
  statement: >
    Technique classification shall achieve at least 0.80 precision and recall
    on every class
  verification: test
  status: not_yet_verified
  reason: >
    Classifier not implemented (phase 8), and the class set is undefined.
    May be unverifiable until labelled examples per class are known.
  metric:
    min_class_precision: {min: 0.80}
    min_class_recall: {min: 0.80}
```

`status: not_yet_verified` with a stated reason is deliberate, and it stays in the report.
An unverified requirement that says why reads as engineering discipline. A requirement
quietly loosened until it passes reads as the opposite, and is the more likely thing to get
caught.

---

## 8. Phase 5 — Report

One artifact per run, HTML.

- Run ID, timestamp, git SHA, config hash
- **Provenance** — source kind (sim / real / synthetic), title and UDP format, send rate,
  track, session type, which faults were injected
- Requirement table — ID, statement, metric, limit, measured value, verdict, margin
- Plots — inter-arrival gap distribution, packet loss against send rate, lap-time error
  distribution, anomaly score timeline overlaid with injected fault windows
- Confusion matrix (once phase 8 exists)
- **Limitations**

The limitations section is written honestly and is not optional:

- the data is simulator output, not a real vehicle
- which conditions were not swept
- where the synthetic generator diverges from captured sessions
- what the `not_yet_verified` requirements mean for the overall claim

That section is what reads as engineering maturity rather than a student project, and it is
the part most people leave out.

---

## 9. Phase 6 — Registry and compare

SQLite.

```
run_id, timestamp, git_sha, config_hash, data_source, source_detail,
send_rate_hz, requirement_id, metric_value, limit_json, verdict, margin
```

```
boxbox compare <run_a> <run_b>
```

Output: margin delta per requirement, flagging any that moved toward failure. This is what
catches a model change that quietly degrades detection.

---

## 10. Metrics discipline

| Replace | With |
|---|---|
| "real-time" | inter-arrival gap distribution, p50 / p95 / p99 |
| "millisecond-accurate lap timing" | lap-time error against simulator reference, as a distribution |
| "anomaly detection" | P_d and false-alarm rate, per injected fault type |
| "classifier accuracy" | precision and recall per class |
| (unstated) | end-to-end processing latency |
| (unstated) | packet loss against configured send rate |

Per-class and per-fault-type numbers are what a test engineer actually asks for.

---

## 11. Claims discipline

This section exists because the prior handoff described hardware capture that never
happened, and then called for publishing that description in a public README and in resume
bullets.

- Name the data source in the README's **first paragraph**. "Telemetry from F1 25 UDP
  output" — not "telemetry capture."
- Claim only what a committed report has measured.
- An unbuilt component is described as planned, or not at all.
- A driver that was written but never run against hardware may be described as *written*,
  never as *used*.
- Resume bullets carry the provenance: "90% detection of injected sensor faults in simulator
  telemetry at under 1 false alarm per lap." The qualifier costs four words and is the
  difference between a strong claim and a false one.

These are defense and aerospace employers whose business is measurement integrity, and a
public repo is checkable by anyone who cares to.

---

## 12. Deliverables

1. Public repo with phases 1–6
2. One committed example report from a real run
3. A README that leads with what was measured, and on what data
4. A resume bullet set with real figures and named provenance

---

## 13. Open questions

- **Which UDP format — `2025` or the 2026 Season Pack?** EA publishes both specifications
  and the packet layouts differ. New players default to the 2026 pack. Pick one and pin it
  in config; do not let it float.
- **Do you have a car, and what model year?** Determines whether phase 9 needs a CAN-FD
  capable STN1110 adapter (2020+) or whether a $15 ELM327 suffices — and whether phase 9 is
  possible at all.
- **Python version and environment strategy** across Windows/NVIDIA and Apple Silicon. One
  lockfile or two? GPU-dependent packages will differ.
- **Which technique classes should the classifier predict?** Undefined. `REQ-CLS-001`
  cannot be written properly until this is answered.
- **Does the review GUI stay PySide6**, given the reports are already HTML? A browser-based
  review view may fall out of phase 5 nearly for free.
