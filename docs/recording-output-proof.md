# Owned concurrent recording mux and delivery

`scripts/recording_output.py` adds a one-use `RecordingOutput` owner around an actual
prepared `RecordingCutInput`. This follows the [recording-local cut proof](recording-cut-proof.md):
input transfer no longer needs to be followed by unowned asynchronous mux/file work.
It remains an opt-in Python/native experiment, not production HDS or a privacy sandbox.

## Ownership and cancellation

Construction validates limits/receiver methods, commits the still-authorized cut and
packs bounded private IPC. `run()` owns the worker, temporary directory, input and
receiver through mux, sequential chunk delivery and an asynchronous finish callback.
No partial mux output is delivered. A successful result is exposed only after receiver
finish **and cleanup**; it is not an iCloud recording acknowledgment.

The lifecycle is ready → muxing → delivering → finishing → complete, or cancelled /
failed. Cancellation preserves its first reason and prevents subsequent callback
invocations or a successful job result. An already invoked callback may have accepted
bytes or begun a completion operation; cancellation cannot retract those side effects.
Callbacks must cooperate with task cancellation. There is no hostile-code isolation.

The input memoryview is released even during blocked pipe input; cleanup collects a
spawned process handle even if cancellation wins during asynchronous process creation.
A separately shielded cleanup task reaps the worker, awaits stdin closure (consuming
expected broken-pipe/reset errors), removes the private temporary directory and clears
managed input/receiver/task/process references. Repeated caller cancellation does not
interrupt that cleanup. Non-cooperative callbacks, filesystem/OS cleanup failures and
uncatchable process termination are not covered by a hard completion-time guarantee.

The source supervisor must explicitly cancel output on privacy/session retirement.
The probe's conservative policy cancels any still-running output when acquisition
retires, **before awaiting worker cleanup**. This policy is orchestration, not an
implicit link created by transferring input copies. A completed receiver transaction
cannot be revoked by later source retirement. External aliases, receiver copies,
exception tracebacks or externally copied files are outside managed ownership.

## Per-job bounds

- Prepared input retains the existing 256-packet / 2-MiB inspection-or-wire budget;
  packed job input is separately checked at **4 MiB**, including IPC length prefixes.
  A conservative inspection-or-wire upper bound is checked before transfer/packing,
  so a larger caller-configured input budget cannot force an oversized job allocation.
- Default output file maximum is **8 MiB**, configurable up to 32 MiB. A small POSIX
  exec wrapper (`recording_output_limit.py`) installs `RLIMIT_FSIZE` **before** the
  native mux executable runs. This is not a polling-based overshoot estimate.
- Default delivery and pipe-write chunks are **64 KiB maximum**. Only one receiver
  write is pending at a time; pipe transport buffering and serialization intermediates
  are additional bounded representations, not a universal zero-copy/RSS claim.
- Default mux/delivery timeout is **10 seconds**, configurable up to 30. It starts at
  `run()`, requests cancellation, and then waits for cooperative cleanup. Ready jobs
  require the owner to start or cancel them; they have no autonomous idle timer.
- Temporary output is confined to a newly created private directory and removed on
  every exercised success/refusal/cancellation path. This is not secure erasure.

These are **per-job**, not fleet-wide bounds. Aggregate recording-job admission,
completed-output retention, concurrent cuts and production storage policy remain open.

## Paced native matrix

`scripts/recording_output_probe.py` starts output immediately after a cut, while the
single native ingest worker and ordinary live subscriber continue. There is no
accumulated input-capture replay. The receiver retains only rolling hashes/counts;
its successful finish checks the delivered bytes against the owned mux file, probes
9 video / 30 AAC packets and decodes exactly original fixture video frames 15–23.

Darwin arm64, Python 3.14.10, FFmpeg 9.0.2:

| Mode | Receiver accepted bytes | Acknowledged write bytes | Finish calls | Outcome |
|---|---:|---:|---:|---|
| Successful concurrent output | 28082 | 28082 | 1 | complete |
| Cancel blocked write | 4096 | 0 | 0 | `privacy` |
| Cancel during finish | 28082 | 28082 | 1 | `privacy`, no successful finish result |
| Blocked-write timeout | 4096 | 0 | 0 | `timeout` |
| Receiver write error | 4096 | 0 | 0 | `output_failed` |
| Native output limit of 64 bytes | 0 | 0 | 0 | `output_failed` |
| Source EOF while write is pending | 4096 | 0 | 0 | `source_failed` |

Seven 4096-byte-or-smaller writes comprise the successful file. At first write the
source had published **94 packets**; at completion of delivery/decoder checks it had
published **105**, with its epoch active and worker still running at both observations.
This demonstrates concurrent progress, not a latency or throughput benchmark.

The first six cases retain exact ordinary-consumer hashes/counts for **187 packets**
(48 video / 139 AAC), including 99 additional packets after the cut. In the source-EOF
case, the ordinary peer receives **125 packets** (33 / 92), 37 after the cut; the
receiver really is blocked before source retirement cancels its job. Every output and
ingest worker is reaped, managed input bytes return to zero, and output files and their
private directories are gone. [Curated observations](recording-output-results.json)
record these checks without retaining media.

A completed callback is not equivalent to accepted bytes: deliberately blocked/error
receivers accept a first chunk before the job can count an acknowledged write. The
finish-cancellation case has already accepted the entire file and entered `finish()`;
no claim is made that it can be undone. No HDS end packet or cloud acknowledgment is
implemented here.

## Validation and remaining work

The opt-in Nix package runs the seven-case native matrix. Full integrations, **206
Python tests**, and **61 Rust tests/doctests** pass. Thirteen focused output tests cover
success/one-use, pre-start cancellation, blocked writes/finish, mux and backpressure
timeouts, output-size enforcement, receiver errors, invalid limits, blocked mux input,
cancellation during spawn, repeated cancellation during cleanup and pre-allocation
input-budget refusal. They also reject
unretrieved asynchronous pipe/task exceptions. The fake sinks used for deterministic
pipe/spawn boundaries are lifecycle controls, not codec evidence.

Existing AAC startup/continuous-PCM differences and whole-AU/common-epoch presentation
remain unchanged. Next: common-origin presentation/edit alternatives, keeping the
exact-clock baseline and decoder differences explicit. Duration-driven/overlapping
cuts, aggregate output admission, real transport backpressure, production Rust
supervision/privacy and physical-camera/Apple live/HSV acceptance remain open.
No camera setting or production process was changed.
