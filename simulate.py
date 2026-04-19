"""
CHIME/FRB pipeline simulator.

Simulates the full CHIME/FRB realtime and post-detection pipeline:

  L1  — 1024 beam processes search each 8-second block in parallel
  L2  — aggregates beam candidates, clusters into astrophysical events
  Post— ring buffer collection, HDF5 conversion, registration, replication

Logs are written as structured JSON to stdout; Grafana Alloy collects them
and forwards to Loki with helix_entity_id / helix_instrument_id as stream labels.

Environment variables:
    GATEWAY_ENDPOINT   gRPC endpoint of the HelixObs gateway (default: gateway:4317)
    BLOCK_INTERVAL_S   Seconds between simulated data blocks (default: 12.0)
    N_BEAMS            Number of L1 beams per block (default: 1024)
    L2_WINDOW_S        Seconds L2 waits for all beam results (default: 1.0)
"""

import logging
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from helixobs.logging import configure_logging
from chime import CHIMEInstrument
from chime.l1 import process_beam
from chime.l2 import cluster, trigger_ring_buffer
from chime.post_detection import convert_to_hdf5, register_event, replicate

# ── Startup ───────────────────────────────────────────────────────────────────

configure_logging()
logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger("chime.simulator")

GATEWAY    = os.environ.get("GATEWAY_ENDPOINT", "gateway:4317")
INTERVAL   = float(os.environ.get("BLOCK_INTERVAL_S", "12.0"))
N_BEAMS    = int(os.environ.get("N_BEAMS", "32"))     # 4 nodes × 8 beams
L2_WINDOW  = float(os.environ.get("L2_WINDOW_S", "1.0"))

RACKS = [f"rack-{i}" for i in range(1, 9)]

tel = CHIMEInstrument(service_name="chime.simulator", endpoint=GATEWAY)
log.info("CHIME simulator starting")

# ── Main pipeline ─────────────────────────────────────────────────────────────


def run_block() -> None:
    block_id = f"block-{uuid.uuid4().hex[:12]}"

    # Ingest block
    block_token = tel.track("x-engine", id=block_id)
    tel.data_block_metadata(
        block_token,
        fpga_rack=random.choice(RACKS),
        freq_min_mhz=400.0,
        freq_max_mhz=800.0,
        n_freq_channels=1024,
        duration_s=8.0,
    )
    log.info("block ingested")
    tel.complete(block_token)

    # L1: run all beams in parallel
    all_cand_ids: list[str] = []
    with ThreadPoolExecutor(max_workers=64) as pool:
        futures = {
            pool.submit(process_beam, tel, block_id, beam_id): beam_id
            for beam_id in range(N_BEAMS)
        }
        for fut in as_completed(futures):
            try:
                all_cand_ids.extend(fut.result())
            except Exception:
                log.exception("beam thread crashed")

    log.info(
        f"L1 complete: {len(all_cand_ids)} candidates across {N_BEAMS} beams"
    )

    # L2: wait for aggregation window, then cluster
    time.sleep(L2_WINDOW)
    event_id = cluster(tel, block_id, all_cand_ids)

    if event_id is None:
        return

    # Trigger upstream ring buffer collection
    trigger_ring_buffer(tel, event_id)

    # Post-detection pipeline
    file_id = convert_to_hdf5(tel, event_id)
    if file_id is None:
        return

    reg_id = register_event(tel, event_id, file_id)
    if reg_id is None:
        return

    replicate(tel, event_id, reg_id)


# ── Run loop ──────────────────────────────────────────────────────────────────

while True:
    try:
        run_block()
    except Exception:
        log.exception("simulation step failed")
    time.sleep(INTERVAL)
