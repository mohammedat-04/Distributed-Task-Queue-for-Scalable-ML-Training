# Distributed Systems Exercises 2025

## Metrics & plots
- Per-epoch loss/accuracy, wall-time, throughput, worker count, and mode are logged to `logs/*.csv` automatically during training.
- Use the producer CLI command `plot` to render `logs/speedup.png` and `logs/sync_async.png` from accumulated runs (speedup expects 1/2/4 worker runs; both sync+async are supported).

## Jobs
- `sum start|end` computes the sum of a range.
- `sumsq start|end` computes the sum of squares of a range.
- `train ...` submits a training job.
- `resume jobId|key=value|...` resumes from saved weights of a previous job.

## Demo jobs 
Use the prebuilt test list in `src/de/luh/vss/chat/dtq/producer/jobs.txt`.  
It contains  copy/paste commands that cover:
1) SUM, 2) SUMSQ, 3–8) MNIST training variants, 9) stock dataset, 10) invalid dataRef (negative test).

Note: the file uses `train/sum/sumsq/resume` without a leading slash.  


## Master CLI
- `jobs` shows job states (with producer ID).
- `list` shows active workers/producers and last lease timestamp.
- `info` shows master IP and port.
- `workers` shows per-worker performance (assigned/completed/failed/inflight + avg task time + lease age).
- `workers verbose` also prints the inflight task IDs and their age.

## Fault tolerance
- Tasks are retried with backoff; after 5 failed attempts the job is marked FAILED.
- Workers send task progress heartbeats; the master only reclaims when progress stops.
