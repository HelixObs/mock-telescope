"""
CHIME pipeline simulator.

Continuously generates fake data blocks → beam candidates → FRB events and
ships them to the HelixObs gateway via OTLP.  Logs are written as structured
JSON to stdout; Grafana Alloy collects them and forwards to Loki.

Environment variables:
    GATEWAY_ENDPOINT   gRPC endpoint of the HelixObs gateway (default: gateway:4317)
    BLOCK_INTERVAL_S   Seconds between simulated data blocks (default: 3.0)
    N_BEAMS            Number of L1 beams per block (default: 32)
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

# ── Logging setup ─────────────────────────────────────────────────────────────

configure_logging()
logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger("chime.simulator")

# ── Instrument ────────────────────────────────────────────────────────────────

GATEWAY   = os.environ.get("GATEWAY_ENDPOINT", "gateway:4317")
INTERVAL  = float(os.environ.get("BLOCK_INTERVAL_S", "3.0"))
N_BEAMS   = int(os.environ.get("N_BEAMS", "32"))
L2_WINDOW = float(os.environ.get("L2_WINDOW_S", "1.0"))

RACKS = [f"rack-{i}" for i in range(1, 9)]

tel = CHIMEInstrument(service_name="chime.simulator", endpoint=GATEWAY)
log.info("CHIME simulator starting")

# ── Main pipeline ─────────────────────────────────────────────────────────────


def run_block() -> None:

    # L1: run all beams in parallel
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

    # L2: wait for aggregation window, then cluster
    time.sleep(L2_WINDOW)
    event_id = cluster(tel, all_cand_ids)

    if event_id is None:
        return

    trigger_ring_buffer(tel, event_id)

    # Post-detection pipeline (child spans of the FRB event)
    if convert_to_hdf5(tel, event_id) is None:
        return

    if register_event(tel, event_id) is None:
        return

    replicate(tel, event_id)


# ── Run loop ──────────────────────────────────────────────────────────────────

while True:
    try:
        run_block()
    except Exception:
        log.exception("simulation step failed")
    time.sleep(INTERVAL)
