"""
L2 pipeline — aggregates beam candidates, clusters into events, runs
scientific classification algorithms, and triggers upstream ring buffer
collection for genuine astrophysical events.
"""

import logging
import random
import time

from . import CHIMEInstrument

log = logging.getLogger("chime.l4")

P_RPC_FAILURE        = 0.05   # 5% of ring buffer RPC calls fail
P_HCO_FAILURE        = 0.10 # 10% of HCO calls fail

def l4(
    tel: CHIMEInstrument,
    event_id: str,
) -> str | None:
    with tel.operate("l4", entity_id=event_id) as op:
        write_header(tel)
        trigger_ring_buffer(tel, event_id)
        trigger_baseband(tel)
        trigger_outriggers(tel)


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

def trigger_baseband(tel):
    with tel.child_span("trigger_baseband") as span:
        time.sleep(0.001)
        log.info("hello from Coco.")

def trigger_outriggers(tel):
    with tel.child_span("trigger_kko") as span:
        time.sleep(0.001)
        log.info("hello from KKO.")
    with tel.child_span("trigger_gbo") as span:
        time.sleep(0.001)
        log.info("hello from GBO.")
    with tel.child_span("trigger_hco") as span:
        time.sleep(0.001)
        if random.random() < P_HCO_FAILURE:
            log.error(
                "Could not connect to HCO. Baseband at HCO is lost."
            )
            span.set_attribute("helix.chime.rpc_status", "timeout")
            span.record_exception(RuntimeError("rpc_timeout"))
        else:
            log.info("hello from HCO.")

def write_header(tel):
    with tel.child_span("write_header") as span:
        time.sleep(0.001)
        log.info("hello from L4 DB.")
