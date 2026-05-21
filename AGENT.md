# mock-telescope

Reference CHIME pipeline simulator — generates a continuous stream of fake radio telescope
data and ships it to the HelixObs gateway via OTLP. Used for development, acceptance
testing, and demo purposes.

## What it does

Simulates the full CHIME FRB detection pipeline in a loop:

1. **L1 search** (per beam, parallel): processes one 8-second data block per beam,
   performs RFI excision and dedispersion, produces beam candidates.
2. **L2 clustering**: aggregates surviving candidates from all beams, clusters into
   astrophysical events, classifies each as FRB / PULSAR / RFI / UNKNOWN.
3. **Post-detection** (operations on confirmed FRBs):
   - `hdf5-conversion` — raw ring buffer data → HDF5 science file
   - `registration` — event recorded in the central catalog
   - `replication` — data copied to offsite HPC clusters (Cedar, Niagara, Narval)

Each stage emits OTLP spans with `helix.*` attributes and uses `CHIMEInstrument` from
the `helixobs` client library. Provenance links flow: data block → beam candidates → FRB event.

## Package layout

```
simulate.py           entry point — main loop orchestrating l1 → l2 → post_detection
chime/
  __init__.py         CHIMEInstrument subclass re-export
  l1.py               process_beam() — beam search, RFI excision, candidate creation
  l2.py               cluster() — multi-beam clustering + classification
                      trigger_ring_buffer() — upstream RPC (child span, not entity)
  post_detection.py   convert_to_hdf5(), register_event(), replicate()
                      — these use tel.operate() (entity operations, not new entities)
tests/
  conftest.py
  (integration tests against a live stack)
```

## Failure modes simulated

| Stage | Failure | Rate |
|---|---|---|
| L1 | FPGA rack dropout | 2% of beams |
| L2 | Clustering timeout (too few beams) | 1% of blocks |
| L2 | Ring buffer RPC timeout | 10% of RPC calls |
| Post-detection | HDF5 write failure (NFS/disk) | 5% |
| Post-detection | Registration conflict (duplicate) | 3% |
| Post-detection | Replication timeout | 8% per destination |

These failure rates are intentionally set high enough that Sherlock's test suite
can reliably find entities with `has_error=true` within a short window.

## helixSource metadata

`register_event` sets `helixSource` and `helixSourceLine` on operation metadata
so Sherlock can fetch the relevant source code via `fetch_github_file` and
`fetch_github_blame`. This is the reference implementation for the `helixSource`
convention.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `HERALD_ENDPOINT` | `gateway:4317` | HelixObs gateway gRPC address |
| `BLOCK_INTERVAL_S` | `3.0` | Seconds between simulated data blocks |
| `N_BEAMS` | `32` | Number of L1 beams per block |
| `L2_WINDOW_S` | `1.0` | Seconds L2 waits for all beam results before clustering |

## Running locally

```bash
pip install -e "."
HERALD_ENDPOINT=localhost:4317 python simulate.py
```

## Extending to a new instrument

Create a new top-level package (e.g. `vla/`) mirroring the `chime/` structure:
1. Subclass `Instrument` from `helixobs` for the domain.
2. Implement pipeline stage functions using `tel.create().start()/complete()/error()` and `tel.operate`.
3. Add a `simulate_<instrument>.py` entrypoint.
4. Add a Docker service in `deploy/docker-compose.yml`.
