"""
Post-detection pipeline — runs after an astrophysical event is confirmed.

Stages (each is its own entity in the provenance DAG):
  1. HDF5 conversion  — raw buffer data → portable science file
  2. Registration     — event recorded in the central catalog
  3. Replication      — data copied to one or more offsite HPC clusters
"""

import logging
import random
import uuid

from . import CHIMEInstrument

log = logging.getLogger("chime.post_detection")

REPLICATION_DESTS = ["cedar", "niagara", "narval"]

P_HDF5_FAILURE           = 0.05   # 5%  of conversions fail (disk/NFS error)
P_REGISTRATION_CONFLICT  = 0.03   # 3%  of registrations hit a duplicate
P_REPLICATION_TIMEOUT    = 0.08   # 8%  of replication transfers time out


def convert_to_hdf5(tel: CHIMEInstrument, event_id: str) -> str | None:
    """Convert raw ring buffer data to HDF5. Returns file entity ID or None on failure."""
    file_id = f"file-{uuid.uuid4().hex[:12]}"
    token = tel.track("hdf5-conversion", id=file_id, parents=[event_id])

    if random.random() < P_HDF5_FAILURE:
        log.error(
            f"HDF5 write failed for event {event_id}: NFS mount unresponsive or scratch disk full, "
            "raw voltage data at risk"
        )
        tel.error(token, metadata={"message": "hdf5_write_error", "event_id": event_id})
        return None

    path    = f"/data/chime/frb/{event_id}.hdf5"
    size_mb = round(random.uniform(200.0, 800.0), 1)
    log.info(f"HDF5 conversion complete: {size_mb:.1f} MB written to {path}")
    tel.file_metadata(token, path=path, size_mb=size_mb)
    tel.complete(token)
    return file_id


def register_event(
    tel: CHIMEInstrument,
    event_id: str,
    file_id: str,
) -> str | None:
    """Register the event in the central catalog. Returns registration entity ID or None."""
    reg_id = f"reg-{uuid.uuid4().hex[:10]}"
    token = tel.track("registration", id=reg_id, parents=[event_id, file_id])

    if random.random() < P_REGISTRATION_CONFLICT:
        log.error(
            f"registration conflict: event {event_id} already exists in catalog — "
            "possible duplicate trigger from overlapping beam coverage"
        )
        tel.error(token, metadata={"message": "registration_conflict", "event_id": event_id})
        return None

    log.info(f"event {event_id} registered in catalog with ID {reg_id}")
    tel.complete(token, metadata={"registration_id": reg_id})
    return reg_id


def replicate(tel: CHIMEInstrument, event_id: str, reg_id: str) -> None:
    """Copy data to offsite HPC clusters. Each destination is a separate entity."""
    dests = random.sample(REPLICATION_DESTS, k=random.randint(1, 2))
    for dest in dests:
        rep_id  = f"rep-{uuid.uuid4().hex[:10]}"
        size_mb = round(random.uniform(200.0, 800.0), 1)
        token   = tel.track("replication", id=rep_id, parents=[reg_id])
        tel.replication_metadata(token, dest=dest, size_mb=size_mb)

        if random.random() < P_REPLICATION_TIMEOUT:
            log.error(
                f"replication to {dest} timed out after partial transfer of {size_mb:.1f} MB "
                "— remote HPC storage may be unavailable"
            )
            tel.error(token, metadata={"message": "replication_timeout", "dest": dest})
        else:
            log.info(f"replication to {dest} complete: {size_mb:.1f} MB transferred successfully")
            tel.complete(token)
