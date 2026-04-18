"""
CHIME pipeline simulator.

Continuously generates fake data blocks → beam candidates → FRB events and
ships them to the HelixObs gateway via OTLP.  Logs are shipped to Loki via
the helixobs LokiHandler so every log line carries the helix entity context.

Environment variables:
    GATEWAY_ENDPOINT   gRPC endpoint of the HelixObs gateway (default: gateway:4317)
    LOKI_URL           Loki base URL for log shipping   (default: http://loki:3100)
    BLOCK_INTERVAL_S   Seconds between simulated data blocks (default: 3.0)
"""

import logging
import os
import random
import time
import uuid

from helixobs.logging import configure_logging, configure_loki
from chime import CHIMEInstrument

# ── Logging setup ─────────────────────────────────────────────────────────────

configure_logging()
configure_loki(
    url=os.environ.get("LOKI_URL", "http://loki:3100"),
    labels={"app": "mock-telescope", "instrument": "CHIME"},
    level=logging.INFO,
)
logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger("chime.simulator")

# ── Instrument ────────────────────────────────────────────────────────────────

GATEWAY  = os.environ.get("GATEWAY_ENDPOINT", "gateway:4317")
INTERVAL = float(os.environ.get("BLOCK_INTERVAL_S", "3.0"))

tel = CHIMEInstrument(service_name="chime.simulator", endpoint=GATEWAY)
log.info("CHIME simulator starting", extra={"gateway": GATEWAY, "interval_s": INTERVAL})

# ── Simulation ────────────────────────────────────────────────────────────────

RACKS = [f"rack-{i}" for i in range(1, 9)]


def ingest_block() -> str:
    block_id = f"block-{uuid.uuid4().hex[:12]}"
    token = tel.track("x-engine", id=block_id)
    tel.data_block_metadata(
        token,
        fpga_rack=random.choice(RACKS),
        freq_min_mhz=400.0,
        freq_max_mhz=800.0,
        n_freq_channels=1024,
        duration_s=8.0,
    )
    tel.complete(token)
    log.info("data block ingested", extra={"block_id": block_id})
    return block_id


def classify_candidates(block_id: str) -> list[str]:
    n_beams = random.choices([0, 1, 2, 3, 4], weights=[50, 20, 15, 10, 5])[0]
    cand_ids: list[str] = []
    for _ in range(n_beams):
        beam    = random.randint(0, 255)
        cand_id = f"cand-{uuid.uuid4().hex[:12]}"
        token   = tel.track("frb-classifier", id=cand_id, parents=[block_id])
        tel.candidate_metadata(
            token,
            beam_id=beam,
            dm=round(random.uniform(50, 1500), 1),
            snr=round(random.uniform(8, 60), 1),
        )
        tel.complete(token)
        log.info("candidate classified", extra={"cand_id": cand_id, "beam": beam})
        cand_ids.append(cand_id)
    return cand_ids


def cluster_event(cand_ids: list[str]) -> None:
    if len(cand_ids) < 2:
        return
    event_id = f"frb-{uuid.uuid4().hex[:12]}"
    token    = tel.track("clustering", id=event_id, parents=cand_ids)
    tel.event_metadata(
        token,
        ra=round(random.uniform(0, 360), 4),
        dec=round(random.uniform(-90, 90), 4),
        classification=random.choices(
            ["FRB", "PULSAR", "RFI", "UNKNOWN"],
            weights=[30, 10, 40, 20],
        )[0],
    )
    tel.complete(token)
    log.info("event clustered", extra={"event_id": event_id, "n_candidates": len(cand_ids)})


# ── Main loop ─────────────────────────────────────────────────────────────────

while True:
    try:
        block_id = ingest_block()
        cand_ids = classify_candidates(block_id)
        cluster_event(cand_ids)
    except Exception:
        log.exception("simulation step failed")
    time.sleep(INTERVAL)
