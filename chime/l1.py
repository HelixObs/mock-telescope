"""
L1 search pipeline — one beam per call, designed to run in a thread pool.

Each beam searches an 8-second block for dispersed radio pulses and produces
zero or more candidate event headers. Failures simulate FPGA rack dropouts
(hardware) and RFI contamination (data quality).
"""

import logging
import random
import uuid

from . import CHIMEInstrument

log = logging.getLogger("chime.l1")

RACKS = [f"rack-{i}" for i in range(1, 9)]


def replay_candidates(tel: "CHIMEInstrument", block_candidates: list[dict]) -> list[str]:
    """Emit one 8-second block of real L1 candidates grouped by beam.

    block_candidates is a list of records from l1_headers.json, all falling
    within the same 8-second window.  Each record must have:
        beam_id, dm, snr, dm_error, time_error, tree_index
    """
    by_beam: dict[int, list[dict]] = {}
    for rec in block_candidates:
        by_beam.setdefault(rec["beam_id"], []).append(rec)

    survivors: list[str] = []
    for beam_id, recs in by_beam.items():
        rack = RACKS[beam_id % len(RACKS)]
        beam_id_str = f"beam-{beam_id}-{uuid.uuid4().hex[:12]}"

        token = tel.create("l1-search", id=beam_id_str).start()
        tel.beam_metadata(token, beam_id=beam_id, rack=rack)

        if random.random() < P_RACK_DROPOUT:
            log.error(f"FPGA rack dropout on {rack}: beam {beam_id} lost")
            token.error(metadata={"message": "fpga_rack_dropout", "rack": rack, "beam": beam_id})
            continue

        for rec in recs:
            cand_id = f"cand-{uuid.uuid4().hex[:12]}"
            cand_token = tel.create("l1-candidate", id=cand_id, parents=[beam_id_str]).start()
            tel.candidate_metadata(
                cand_token,
                beam_id=rec["beam_id"],
                dm=rec["dm"],
                snr=rec["snr"],
                dm_error=rec["dm_error"],
                time_error=rec["time_error"],
                tree_index=rec["tree_index"],
            )
            cand_token.complete()
            survivors.append(cand_id)

        token.complete(metadata={"n_candidates": len(recs)})

    return survivors

P_RACK_DROPOUT      = 0.02   # 2%  of beams lose their FPGA rack
P_BEAM_HAS_SIGNAL   = 1.0   # 100% of beams find candidates (~8 of 32)


def process_beam(tel: CHIMEInstrument, beam_id: int) -> list[str]:
    """Search one beam for candidates. Returns surviving candidate entity IDs."""
    rack = RACKS[beam_id % len(RACKS)]
    beam_id_str = f"beam-{beam_id}-{uuid.uuid4().hex[:12]}"

    token = tel.create("l1-search", id=beam_id_str).start()
    tel.beam_metadata(token, beam_id=beam_id, rack=rack)

    if random.random() < P_RACK_DROPOUT:
        log.error(
            f"FPGA rack dropout on {rack}: beam {beam_id} lost all visibility, "
            "aborting block processing"
        )
        token.error(metadata={"message": "fpga_rack_dropout", "rack": rack, "beam": beam_id})
        return []

    # RFI excision: flag bad channels before dedispersion.
    with tel.child_span("rfi_mitigation") as span:
        n_flagged = random.randint(0, 12)
        span.set_attribute("helix.chime.rfi_flagged_channels", n_flagged)
        log.info(f"RFI mitigation complete: {n_flagged} channel(s) excised on beam {beam_id}")

    # Dedispersion over DM trial grid + boxcar matched filter.
    with tel.child_span("trigger_search") as span:
        dm_trials = random.randint(512, 2048)
        span.set_attribute("helix.chime.dm_trials", dm_trials)
        span.set_attribute("helix.chime.signal_found", random.random() < P_BEAM_HAS_SIGNAL)
        log.info(f"dedispersion search complete: {dm_trials} DM trials evaluated on beam {beam_id}")

    survivors: list[str] = []

    if random.random() < P_BEAM_HAS_SIGNAL:
        n = random.choices([1, 2, 3], weights=[60, 30, 10])[0]

        # Single-beam clustering groups triggers into candidate events.
        with tel.child_span("per_beam_clustering") as span:
            span.set_attribute("helix.chime.n_clusters", n)
            for _ in range(n):
                cand_id = f"cand-{uuid.uuid4().hex[:12]}"
                snr = round(random.uniform(8.0, 80.0), 1)
                dm  = round(random.uniform(50.0, 2000.0), 1)

                cand_token = tel.create("l1-candidate", id=cand_id, parents=[beam_id_str]).start()
                tel.candidate_metadata(cand_token, beam_id=beam_id, dm=dm, snr=snr)
                log.info(f"L1 candidate passed RFI veto: SNR={snr}, DM={dm:.1f} pc/cm³, beam={beam_id}")
                cand_token.complete()
                survivors.append(cand_id)

    log.info(
        f"beam {beam_id} on {rack} complete: {len(survivors)} surviving candidate(s) forwarded to L2"
    )
    token.complete(metadata={"n_candidates": len(survivors)})
    return survivors
