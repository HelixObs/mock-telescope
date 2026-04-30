"""
L2 pipeline — aggregates beam candidates, clusters into events, runs
scientific classification algorithms, and triggers upstream ring buffer
collection for genuine astrophysical events.
"""

import logging
import random
import uuid
import time

from . import CHIMEInstrument

log = logging.getLogger("chime.l2")


P_CLUSTERING_TIMEOUT = 0.01   # 1%  of blocks: insufficient beams reported in window


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
        token = tel.create("l2-clustering", id=cluster_id).start()
        log.error(
            f"L2 clustering timeout: only {len(all_cand_ids)} candidate(s) received "
            "within aggregation window — too few beams reporting, skipping block"
        )
        token.error(metadata={
            "message":      "clustering_timeout",
            "n_candidates": len(all_cand_ids),
        })
        return None

    event_id = f"frb-{uuid.uuid4().hex[:12]}"
    # Sample a representative subset of candidates as parents.
    k = min(random.choices([1, 2, 3], weights=[50, 40, 10])[0], len(all_cand_ids))
    parents = random.sample(all_cand_ids, k) if k > 0 else []
    token = tel.create("l2-l3", id=event_id, parents=parents).start()

    # Call RFI sifter
    rfi_sifter(tel)

    # Call localizer
    localizer(tel)

    # Call DM checker
    dm_checker(tel)

    # Can known source sifter
    known_source_sifter(tel)

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
        token.complete()
        return None

    log.info(
        f"astrophysical event confirmed: {classification} with {len(all_cand_ids)} "
        "contributing candidate(s) — triggering post-detection pipeline"
    )
    token.add_event("helix.event.candidate_promoted", {"classification": classification})

    # Call Action picker
    action_picker(tel)
    token.complete()
    return event_id

def rfi_sifter(tel):
    with tel.child_span("rfi_sifter") as span:
        time.sleep(0.001)
        log.info("hello from RFI sifter.")

def localizer(tel):
    with tel.child_span("localizer") as span:
        time.sleep(0.001)
        log.info("hello from localizer.")

def dm_checker(tel):
    with tel.child_span("dm_checker") as span:
        time.sleep(0.001)
        log.info("hello from DM checker.")

def known_source_sifter(tel):
    with tel.child_span("known_source_sifter") as span:
        time.sleep(0.001)
        log.info("hello from Known Source Sifter.")

def action_picker(tel):
    with tel.child_span("action_picker") as span:
        time.sleep(0.001)
        log.info("hello from Action Picker.")