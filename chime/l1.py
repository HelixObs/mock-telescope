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

P_RACK_DROPOUT      = 0.02   # 2%  of beams lose their FPGA rack
P_BEAM_HAS_SIGNAL   = 1.0   # 100% of beams find candidates (~8 of 32)


def process_beam(tel: CHIMEInstrument, block_id: str, beam_id: int) -> list[str]:
    """Search one beam for candidates. Returns surviving candidate entity IDs."""
    rack = RACKS[beam_id % len(RACKS)]
    beam_id_str = f"beam-{uuid.uuid4().hex[:12]}"

    token = tel.track("l1-search", id=beam_id_str, parents=[block_id])
    tel.beam_metadata(token, beam_id=beam_id, rack=rack)

    if random.random() < P_RACK_DROPOUT:
        log.error(
            f"FPGA rack dropout on {rack}: beam {beam_id} lost all visibility, "
            "aborting block processing"
        )
        tel.error(token, metadata={"message": "fpga_rack_dropout", "rack": rack, "beam": beam_id})
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

                cand_token = tel.track("l1-candidate", id=cand_id, parents=[beam_id_str])
                tel.candidate_metadata(cand_token, beam_id=beam_id, dm=dm, snr=snr)
                log.info(f"L1 candidate passed RFI veto: SNR={snr}, DM={dm:.1f} pc/cm³, beam={beam_id}")
                tel.complete(cand_token)
                survivors.append(cand_id)

    log.info(
        f"beam {beam_id} on {rack} complete: {len(survivors)} surviving candidate(s) forwarded to L2"
    )
    tel.complete(token, metadata={"n_candidates": len(survivors)})
    return survivors
