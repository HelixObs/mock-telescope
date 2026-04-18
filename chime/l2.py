"""
L2 pipeline — aggregates beam candidates, clusters into events, runs
scientific classification algorithms, and triggers upstream ring buffer
collection for genuine astrophysical events.
"""

import logging
import random
import uuid

from . import CHIMEInstrument

log = logging.getLogger("chime.l2")

MIN_CANDIDATES_FOR_EVENT = 2

P_CLUSTERING_TIMEOUT = 0.01   # 1%  of blocks: insufficient beams reported in window
P_RPC_FAILURE        = 0.10   # 10% of ring buffer RPC calls fail


def cluster(
    tel: CHIMEInstrument,
    block_id: str,
    all_cand_ids: list[str],
) -> str | None:
    """
    Cluster surviving L1 candidates into an astrophysical event.

    Returns the event entity ID if a real astrophysical event was formed,
    None if the block produced no event or was classified as non-astrophysical.
    """
    if random.random() < P_CLUSTERING_TIMEOUT:
        cluster_id = f"l2cl-{uuid.uuid4().hex[:10]}"
        token = tel.track("l2-clustering", id=cluster_id, parents=[block_id])
        log.error("L2 clustering timeout: insufficient beams reported in window")
        tel.error(token, metadata={
            "message":      "clustering_timeout",
            "n_candidates": len(all_cand_ids),
        })
        return None

    if len(all_cand_ids) < MIN_CANDIDATES_FOR_EVENT:
        return None

    event_id = f"frb-{uuid.uuid4().hex[:12]}"
    # Cap parents to avoid oversized spans when many candidates are present.
    parents = all_cand_ids[:20]
    token = tel.track("l2-clustering", id=event_id, parents=parents)

    classification = random.choices(
        ["FRB", "PULSAR", "RFI", "UNKNOWN"],
        weights=[25, 10, 45, 20],
    )[0]

    tel.event_metadata(
        token,
        ra=round(random.uniform(0, 360), 4),
        dec=round(random.uniform(-90, 90), 4),
        classification=classification,
    )

    if classification in ("RFI", "UNKNOWN"):
        log.info("event classified — not astrophysical, no post-detection")
        tel.complete(token)
        return None

    log.info("astrophysical event clustered")
    token.add_event("helix.event.candidate_promoted", {"classification": classification})
    tel.complete(token)
    return event_id


def trigger_ring_buffer(tel: CHIMEInstrument, event_id: str) -> None:
    """
    RPC call to upstream node to pull raw data from ring buffers.
    Runs synchronously in the mock; in production this would be fire-and-forget.
    """
    rpc_id = f"rpc-{uuid.uuid4().hex[:10]}"
    token = tel.track("ring-buffer-rpc", id=rpc_id, parents=[event_id])

    if random.random() < P_RPC_FAILURE:
        log.warning("ring buffer RPC failed")
        tel.error(token, metadata={"message": "rpc_timeout"})
    else:
        size_mb = round(random.uniform(100.0, 500.0), 1)
        log.info("ring buffer data collected")
        tel.complete(token, metadata={"buffer_size_mb": size_mb})
