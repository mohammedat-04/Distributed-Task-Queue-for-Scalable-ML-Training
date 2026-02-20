# Distributed Task Queue (DTQ) - DS Project 2025

A Java-based distributed systems project with three cooperating roles:

- `Master`: accepts jobs, splits them into tasks, schedules workers, and aggregates results.
- `Worker`: requests tasks, executes compute jobs, and reports progress/results.
- `Producer`: interactive CLI client to submit jobs, inspect states, and visualize metrics.

The system supports numeric jobs (`SUM`, `SUMSQ`) and distributed mini-batch training (`TrainGdBatchJob`) with `sync` and `async` modes.

## Features

- Distributed task scheduling over TCP sockets.
- Job types:
  - range sum (`sum`)
  - range sum of squares (`sumsq`)
  - gradient-based training (`train`)
- Fault tolerance:
  - lease-based liveness checks
  - retries with exponential backoff
  - max-attempt protection
  - progress-aware task reclaim
  - speculative execution for stragglers
- Metrics and artifacts:
  - per-epoch CSV logs
  - job summary and speedup logs
  - generated PNG charts
  - exported model weights (CSV and 28x28 image for MNIST-size vectors)
- State persistence for master, producer, and worker processes.

## Tech Stack

- Java 24 (project classpath is set to `JavaSE-24`)
- Standard library only (no external runtime dependencies)
- Eclipse-style project layout (`src/` -> `bin/`)

## Project Structure

```text
src/de/luh/vss/chat/
  common/                  # message protocol and role types
  dtq/
    master/                # scheduler, leases, task lifecycle, aggregation
    worker/                # worker runtime and training task execution
    producer/              # producer CLI, result handling, plotting trigger
    job/                   # job models (including TrainGdBatchJob)
    task/                  # task state model
    metrics/               # CSV logging + chart generation
```

Important runtime output folders (created automatically):

- `logs/` -> epoch logs, summaries, speedup CSV, generated charts
- `state/` -> persistent state snapshots per component
- project root -> `weights_<jobId>.csv` and optionally `weights_<jobId>.png`

## Prerequisites

- JDK 24 installed and available on `PATH`
- Terminal access (run 3+ terminals for full demo: master, workers, producer)

Verify Java:

```bash
java -version
javac -version
```

## Configuration (Important)

`Worker` and `Producer` currently use a hardcoded master host:

- `src/de/luh/vss/chat/dtq/worker/Worker.java`
- `src/de/luh/vss/chat/dtq/producer/Producer.java`

By default they connect to:

```java
"10.172.119.178", 44444
```

For local testing on one machine, change host to `127.0.0.1` (or your master machine IP on LAN).

## Build

From repository root:

```bash
mkdir -p bin
javac -d bin $(find src -name "*.java")
```

## Run

Start components in this order.

1) Start master:

```bash
java -cp bin de.luh.vss.chat.dtq.master.Master
```

2) Start one or more workers (each with unique worker ID):

```bash
java -cp bin de.luh.vss.chat.dtq.worker.Worker 1 2
java -cp bin de.luh.vss.chat.dtq.worker.Worker 2 2
```

Worker args:

- `arg0`: worker ID (int)
- `arg1`: slots/concurrency (int)
- `arg2` (optional): task timeout in ms for training tasks

3) Start producer:

```bash
java -cp bin de.luh.vss.chat.dtq.producer.Producer 1
```

Producer arg:

- `arg0`: producer ID (int)

## CLI Commands

### Producer CLI

- `help`
- `sum start|end`
- `sumsq start|end`
- `train key=value|key=value|...`
- `resume jobId|key=value|...`
- `jobs`
- `plot`
- `chat <message>`
- `exit`

### Master CLI

- `jobs` -> list job states and producer IDs
- `list` -> list active workers/producers with lease timestamps
- `info` -> show master IP and port
- `workers` -> worker performance snapshot
- `workers verbose` -> include in-flight task ages
- `exit` -> persist completed jobs and shut down

### Worker CLI

- `stats` -> local worker stats (completed/failed/last task)
- `REQUEST_TASK` -> manual task request
- `<text>` -> sends chat text
- `exit`

## Job Examples

```text
sum 1|1000000
sumsq 1|1000000
```

Training example:

```text
train type=TrainGdBatchJob|model=logreg|task=binary_is_zero|samples=2000|features=784|epochs=1|batchSize=128|lr=0.1|seed=42|dataRef=file:./data/train.csv|testRef=file:./data/mnist_test.csv|hasHeader=true|csvLabel=first|normalize=255|init=zeros|mode=sync
```

Resume from previous model weights:

```text
resume <jobId>|jobId=<newJobId>|epochs=1|lr=0.01|batchSize=128
```

Prebuilt demo commands are available in:

- `src/de/luh/vss/chat/dtq/producer/jobs.txt`

## Train Payload Reference

Required fields for `train`:

- `model` (`linreg` or `logreg`)
- `task` (for example `binary_is_zero`, `binary_is_one`, ...)
- `samples`
- `features`
- `epochs`
- `batchSize`
- `lr`
- `seed`
- `dataRef`

Common optional fields:

- `jobId`
- `testRef`
- `hasHeader` (`true`/`false`)
- `csvLabel` (`first` or `last`)
- `normalize` (for example `255`)
- `mode` (`sync` or `async`)
- `init` (`zeros` or `weights`)
- `w` (comma-separated weight vector)
- `wFile` (path to a local weights file, producer side convenience)
- `patience`, `minDelta` (early stopping behavior)

## Data Files

Datasets used by training jobs are included under:

- `src/de/luh/vss/chat/dtq/worker/data`
- `src/de/luh/vss/chat/dtq/producer/data`

`dataRef`/`testRef` resolution supports:

- direct path
- `file:` prefixed path
- fallback to worker data folders when relative paths are provided

## Metrics, Plots, and Artifacts

During/after training, the system writes:

- `logs/<jobId>_epochs.csv`
- `logs/job_summaries.csv`
- `logs/speedup.csv`
- `logs/speedup.png` (via `plot`)
- `logs/sync_async.png` (via `plot`)
- `weights_<jobId>.csv`
- `weights_<jobId>.png` (when weights length is `784`)

## Fault Tolerance and Scheduling Notes

- Lease renewals every 5s; stale peers are removed by master lease checks.
- Running tasks have leases and progress heartbeats.
- Reclaim triggers include worker death, stale progress, or long-running tasks.
- Retries use exponential backoff and stop after max attempts.
- Speculative re-execution can launch duplicate work for slow tasks; first valid completion wins.

## Persistence

State snapshots are persisted under `state/`:

- `state/master_state.properties`
- `state/producer_<id>.properties`
- `state/worker_<id>.properties`

Completed/failed jobs are reloaded on restart.

## Troubleshooting

- Worker/producer cannot connect:
  - verify master is running
  - verify host IP in `Worker.java` and `Producer.java`
  - verify port `44444` is reachable
- Job fails with `FAILED|reason=DATAREF`:
  - verify `dataRef`/`testRef` paths exist and are readable
- No charts generated:
  - run at least one training job first, then run `plot` in producer
