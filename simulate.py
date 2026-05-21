"""
CHIME pipeline simulator.

Two modes:
  - Replay (L1_REPLAY_FILE set): loops through real l1_headers.json data,
    preserving the original time structure of source transits.  Inter-block
    gaps are scaled by BLOCK_INTERVAL_S / 8.0 so you can run faster or
    slower than real time.
  - Random (default): generates synthetic candidates as before.

Environment variables:
    HERALD_ENDPOINT    gRPC endpoint of the HelixObs herald (default: herald:4317)
    BLOCK_INTERVAL_S   Random mode only: seconds between blocks (default: 3.0)
    N_BEAMS            Random mode only: beams per block (default: 32)
    L2_WINDOW_S        Seconds L2 waits before clustering (default: 1.0)
    L1_REPLAY_FILE     Path to l1_headers.json; enables replay mode when set.
    REPLAY_SPEED       Replay mode only: playback multiplier (default: 1.0).
                       2.0 = 2× real-time (gaps halved), 0.5 = half real-time.
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from helixobs import setup
from chime import CHIMEInstrument
from chime.l1 import process_beam, replay_candidates
from chime.l2 import cluster
from chime.l4 import l4
from chime.post_detection import convert_to_hdf5, register_event, replicate

# ── Config ────────────────────────────────────────────────────────────────────

HERALD           = os.environ.get("HERALD_ENDPOINT", "herald:4317")
INTERVAL         = float(os.environ.get("BLOCK_INTERVAL_S", "3.0"))
N_BEAMS          = int(os.environ.get("N_BEAMS", "32"))
L2_WINDOW        = float(os.environ.get("L2_WINDOW_S", "1.0"))
L1_REPLAY_FILE   = os.environ.get("L1_REPLAY_FILE", "")
REPLAY_SPEED     = float(os.environ.get("REPLAY_SPEED", "1.0"))

tel = setup(
    "chime.simulator",
    endpoint=HERALD,
    otlp=True,
    instrument_class=CHIMEInstrument,
)
logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger("chime.simulator")

# ── Replay loader ─────────────────────────────────────────────────────────────


def _load_replay_blocks(path: str) -> list[tuple[float, list[dict]]]:
    """Load l1_headers.json and group records into 8-second blocks.

    Returns a list of (block_start_seconds_from_day_start, [records]) sorted
    by time.  block_start is the offset in seconds from the first record,
    used to compute inter-block sleep durations.
    """
    log.info(f"Loading replay data from {path} …")
    with open(path) as f:
        records = json.load(f)

    records.sort(key=lambda r: r["timestamp_utc"])
    t0 = datetime.fromisoformat(records[0]["timestamp_utc"]).timestamp()

    blocks: dict[int, list[dict]] = {}
    for rec in records:
        t = datetime.fromisoformat(rec["timestamp_utc"]).timestamp()
        idx = int((t - t0) / 8)
        blocks.setdefault(idx, []).append(rec)

    # Build sorted list of (block_offset_seconds, candidates)
    sorted_blocks = [(idx * 8.0, cands) for idx, cands in sorted(blocks.items())]
    log.info(f"Loaded {len(records):,} candidates in {len(sorted_blocks):,} blocks "
             f"spanning {sorted_blocks[-1][0] / 3600:.1f} h")
    return sorted_blocks


REPLAY_BLOCKS: list[tuple[float, list[dict]]] = []
if L1_REPLAY_FILE and os.path.isfile(L1_REPLAY_FILE):
    REPLAY_BLOCKS = _load_replay_blocks(L1_REPLAY_FILE)
elif L1_REPLAY_FILE:
    log.warning(f"L1_REPLAY_FILE={L1_REPLAY_FILE!r} is not a regular file, falling back to random mode")

# ── Block runners ──────────────────────────────────────────────────────────────


def run_random_block() -> None:
    """Original random-generation mode."""
    all_cand_ids: list[str] = []
    with ThreadPoolExecutor(max_workers=N_BEAMS) as pool:
        futures = {
            pool.submit(process_beam, tel, beam_id): beam_id
            for beam_id in range(N_BEAMS)
        }
        for fut in as_completed(futures):
            try:
                all_cand_ids.extend(fut.result())
            except Exception:
                log.exception("beam thread crashed")

    log.info(f"L1 complete: {len(all_cand_ids)} candidates across {N_BEAMS} beams")
    _run_l2_and_post(all_cand_ids)


def run_replay_block(block_candidates: list[dict]) -> None:
    """Replay one 8-second block of real candidates."""
    cand_ids = replay_candidates(tel, block_candidates)
    log.info(f"L1 replay: {len(cand_ids)} candidates from {len(set(r['beam_id'] for r in block_candidates))} beams")
    _run_l2_and_post(cand_ids)


def _run_l2_and_post(cand_ids: list[str]) -> None:
    time.sleep(L2_WINDOW)
    event_id = cluster(tel, cand_ids)
    if event_id is None:
        return
    l4(tel, event_id)
    if convert_to_hdf5(tel, event_id) is None:
        return
    if register_event(tel, event_id) is None:
        return
    replicate(tel, event_id)


# ── Run loop ──────────────────────────────────────────────────────────────────

log.info("CHIME simulator starting" + (f" (replay: {L1_REPLAY_FILE})" if L1_REPLAY_FILE else " (random)"))

if not REPLAY_BLOCKS:
    # Random mode — original behaviour
    while True:
        try:
            run_random_block()
        except Exception:
            log.exception("simulation step failed")
        time.sleep(INTERVAL)
else:
    # Replay mode: loop through blocks forever.
    # gap = real_gap_seconds / REPLAY_SPEED
    # REPLAY_SPEED=1 → real-time, REPLAY_SPEED=2 → 2× faster (gaps halved)
    while True:
        for i, (block_offset, block_candidates) in enumerate(REPLAY_BLOCKS):
            try:
                run_replay_block(block_candidates)
            except Exception:
                log.exception("replay block failed")

            if i + 1 < len(REPLAY_BLOCKS):
                next_offset = REPLAY_BLOCKS[i + 1][0]
                gap = (next_offset - block_offset) / REPLAY_SPEED
            else:
                gap = 8.0 / REPLAY_SPEED  # brief pause before looping
            time.sleep(max(gap, 0.05))
