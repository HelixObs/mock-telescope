"""
Post-detection pipeline — runs after an astrophysical event is confirmed.

Each stage is an independent operation on the FRB event entity, running in
its own trace.  The entity (frb-...) is not re-created; operations are linked
to it via entity_id and appear in the Entity Inspector's Trace panel.

  hdf5-conversion  — raw buffer data → portable science file
  registration     — event recorded in the central catalog
  replication      — data copied to one or more offsite HPC clusters
"""

import logging
import random

from . import CHIMEInstrument

log = logging.getLogger("chime.post_detection")

REPLICATION_DESTS = ["cedar", "niagara", "narval"]

P_HDF5_FAILURE           = 0.05   # 5%  of conversions fail (disk/NFS error)
P_REGISTRATION_CONFLICT  = 0.03   # 3%  of registrations hit a duplicate
P_REPLICATION_TIMEOUT    = 0.08   # 8%  of replication transfers time out


def convert_to_hdf5(tel: CHIMEInstrument, event_id: str) -> str | None:
    """Convert raw ring buffer data to HDF5. Returns event_id or None on failure."""
    with tel.operate("hdf5-conversion", entity_id=event_id) as op:
        if random.random() < P_HDF5_FAILURE:
            log.error(
                f"HDF5 write failed for event {event_id}: NFS mount unresponsive or scratch disk full, "
                "raw voltage data at risk"
            )
            op.fail("hdf5_write_error")
            return None

        path    = f"/data/chime/frb/{event_id}.hdf5"
        size_mb = round(random.uniform(200.0, 800.0), 1)
        op.set_attribute("helix.chime.hdf5_path", path)
        op.set_attribute("helix.chime.hdf5_size_mb", size_mb)
        log.info(f"HDF5 conversion complete: {size_mb:.1f} MB written to {path}")
    return event_id


def register_event(tel: CHIMEInstrument, event_id: str) -> str | None:
    """Register the event in the central catalog. Returns event_id or None on conflict."""
    with tel.operate("registration", entity_id=event_id) as op:
        if random.random() < P_REGISTRATION_CONFLICT:
            log.error(
                f"registration conflict: event {event_id} already exists in catalog — "
                "possible duplicate trigger from overlapping beam coverage"
            )
            op.fail("registration_conflict", metadata={
                "helixSource": "https://github.com/HelixObs/mock-telescope/blob/main/chime/post_detection.py",
                "helixSourceLine": 55,
            })
            return None

        op.set_attribute("helix.chime.registration_status", "ok")
        log.info(f"event {event_id} registered in catalog")
    return event_id


def replicate(tel: CHIMEInstrument, event_id: str) -> None:
    """Copy data to offsite HPC clusters. Each destination is its own operation + trace."""
    dests = random.sample(REPLICATION_DESTS, k=random.randint(1, 2))
    for dest in dests:
        size_mb = round(random.uniform(200.0, 800.0), 1)
        with tel.operate("replication", entity_id=event_id) as op:
            op.set_attribute("helix.chime.replication_dest", dest)
            op.set_attribute("helix.chime.replication_size_mb", size_mb)

            if random.random() < P_REPLICATION_TIMEOUT:
                log.error(
                    f"replication to {dest} timed out after partial transfer of {size_mb:.1f} MB "
                    "— remote HPC storage may be unavailable"
                )
                op.fail("replication_timeout")
            else:
                log.info(f"replication to {dest} complete: {size_mb:.1f} MB transferred successfully")
