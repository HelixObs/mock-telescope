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


P_CLUSTERING_TIMEOUT = 0.01   # 1%  of blocks: insufficient beams reported in window
P_RPC_FAILURE        = 0.10   # 10% of ring buffer RPC calls fail


def cluster(
    tel: CHIMEInstrument,
    all_cand_ids: list[str],
) -> str | None:
    """
    Cluster surviving L1 candidates into an astrophysical event.

    Returns the event entity ID if a real astrophysical event was formed,
    None if the block produced no event or was classified as non-astrophysical.
    """
    if random.random() < P_CLUSTERING_TIMEOUT:
        cluster_id = f"l2cl-{uuid.uuid4().hex[:10]}"
        token = tel.track("l2-clustering", id=cluster_id)
        log.error(
            f"L2 clustering timeout: only {len(all_cand_ids)} candidate(s) received "
            "within aggregation window — too few beams reporting, skipping block"
        )
        tel.error(token, metadata={
            "message":      "clustering_timeout",
            "n_candidates": len(all_cand_ids),
        })
        return None

    event_id = f"frb-{uuid.uuid4().hex[:12]}"
    # Sample a representative subset of candidates as parents.
    k = min(random.choices([1, 2, 3], weights=[50, 40, 10])[0], len(all_cand_ids))
    parents = random.sample(all_cand_ids, k) if k > 0 else []
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
        log.info(
            f"event classified as {classification} — not astrophysical, "
            f"discarding {len(all_cand_ids)} candidate(s)"
        )
        tel.complete(token)
        return None

    log.info(
        f"astrophysical event confirmed: {classification} with {len(all_cand_ids)} "
        "contributing candidate(s) — triggering post-detection pipeline"
    )
    token.add_event("helix.event.candidate_promoted", {"classification": classification})
    tel.complete(token)
    return event_id


def trigger_ring_buffer(tel: CHIMEInstrument, event_id: str) -> None:
    """
    RPC call to upstream node to pull raw data from ring buffers.
    This is an internal L2 step — it appears as a child span of the event
    entity's trace rather than a separate HelixObs entity.
    """
    with tel.child_span("ring-buffer-rpc", parent_id=event_id) as span:
        if random.random() < P_RPC_FAILURE:
            log.error(
                "ring buffer RPC timeout: upstream correlator node did not respond "
                "within collection window — raw voltages may be lost"
            )
            span.set_attribute("helix.chime.rpc_status", "timeout")
            span.record_exception(RuntimeError("rpc_timeout"))
        else:
            size_mb = round(random.uniform(100.0, 500.0), 1)
            span.set_attribute("helix.chime.buffer_size_mb", size_mb)
            log.info(f"ring buffer data collected: {size_mb:.1f} MB retrieved from correlator")
