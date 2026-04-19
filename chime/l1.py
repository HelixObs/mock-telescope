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
P_BEAM_HAS_SIGNAL   = 0.25   # 25% of beams find candidates (~8 of 32)
P_CANDIDATE_RFI     = 0.30   # 30% of candidates are RFI-flagged


def process_beam(tel: CHIMEInstrument, block_id: str, beam_id: int) -> list[str]:
    """Search one beam for candidates. Returns surviving candidate entity IDs."""
    rack = RACKS[beam_id % len(RACKS)]
    beam_id_str = f"beam-{uuid.uuid4().hex[:12]}"

    token = tel.track("l1-search", id=beam_id_str, parents=[block_id])
    tel.beam_metadata(token, beam_id=beam_id, rack=rack)

    if random.random() < P_RACK_DROPOUT:
        log.warning("FPGA rack dropout")
        tel.error(token, metadata={"message": "fpga_rack_dropout", "rack": rack, "beam": beam_id})
        return []

    survivors: list[str] = []

    if random.random() < P_BEAM_HAS_SIGNAL:
        n = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
        for _ in range(n):
            cand_id = f"cand-{uuid.uuid4().hex[:12]}"
            snr = round(random.uniform(8.0, 80.0), 1)
            dm  = round(random.uniform(50.0, 2000.0), 1)

            cand_token = tel.track("l1-candidate", id=cand_id, parents=[beam_id_str])
            tel.candidate_metadata(cand_token, beam_id=beam_id, dm=dm, snr=snr)

            if random.random() < P_CANDIDATE_RFI:
                log.warning("candidate flagged as RFI")
                tel.error(cand_token, metadata={"message": "rfi_flagged", "snr": snr, "beam": beam_id})
            else:
                log.info("L1 candidate found")
                tel.complete(cand_token)
                survivors.append(cand_id)

    log.info("beam processed")
    tel.complete(token, metadata={"n_candidates": len(survivors)})
    return survivors
