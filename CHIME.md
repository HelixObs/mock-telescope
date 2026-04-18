# Instructions to create a mock version of CHIME/FRB.

CHIME/FRB detects Fast Radio Bursts in the sky. These bursts are the primary detections called "events".
Once detected, a lot of analysis happens on this so called event.
In addition, CHIME/FRB does other types of analysis like Calibration where it looks at known bright
astrophysical sources to calibrate the instrument.

Obviously, we don't want to implement it exactly but this is what I want my mock system to look like.

## High level System Design for Mock CHIME/FRB.
CHIME/FRB has multiple parts.
- A realtime pipeline
- A verification flow to hand classify events  
- Data Conversion, Registration and Replication.


### The realtime pipeline

For the mock version, lets assume that the realtime pipeline starts from the L1 search pipeline.
There are 1024 processes (one beam per process) which processes and searches an 8 second block (1024 freq X 8192 ms)
of data. From that data, it creates event headers. Those event headers from all those 1024 processes
are streamed to a single process on a different node (called L2 pipeine) over tcp/IP socket.
The L2 pipeline first waits for 4 seconds to let information from that 8 second block to all arrive.
On that batch, it does a clustering on the event headers and creates the final "event". Once the event is formed
a variety of scientific algorithms run on the same event (again in real time) to understand what it is.
If the event is found to be a genuine Fast Radio Burst, it triggers realtime RPC calls to the upstream system
to collect data from their ring buffers. This is where the realtime pipeline analysis for that event ends.

### Verification flow to hand classify events

Once the events are captured, they are also sent to our internal webplatform for a double check.
Before that, the captured data is processed to create a diagnostic plot which the students can verify.


### Data Conversion, Registration and Replication

Once these event data are collected, they are converted into an appropriate hdf5 file format.
Later, after one day, another job called the Registration looks at these data and registers them. The
goal is to likely replicate them to our offsite storage if the event was verified manually as genuine.
The Replicators run asynchronously. Once the data are registered, the replicators will identify and 
copy them offsite.

