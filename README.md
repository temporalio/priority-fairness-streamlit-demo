# Priority & Fairness Demo

Interactive demo of Temporal's task queue priority and fairness features using a Streamlit dashboard.

![Demo](demo.gif)

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- [Temporal CLI](https://docs.temporal.io/cli)

## Setup

```bash
uv sync
```

## Running

### 1. Start the Temporal dev server

Priority and fairness require specific dynamic config flags:

```bash
temporal server start-dev \
    --dynamic-config-value matching.useNewMatcher=true \
    --dynamic-config-value matching.enableFairness=true \
    --dynamic-config-value matching.numTaskqueueReadPartitions=1 \
    --dynamic-config-value matching.numTaskqueueWritePartitions=1
```

### 2. Start the worker

```bash
unset TEMPORAL_PROFILE
uv run python worker.py
```

### 3. Start the dashboard

```bash
unset TEMPORAL_PROFILE
uv run streamlit run dashboard.py
```

`unset TEMPORAL_PROFILE` ensures you connect to the local dev server instead of Temporal Cloud.

## What it does

The dashboard has two tabs:

**Priority** — Start a batch of workflows with random priorities (P1 High, P3 Medium, P5 Low). With priority enabled, high priority workflows execute before low priority ones. Toggle priority off to see FIFO behavior.

**Fairness** — Start a batch of workflows split across three tenants (Big 60%, Mid 25%, Small 15%). With fairness enabled, all tenants get regular turns. Toggle fairness off to see the "noisy neighbor" problem where the big tenant starves the others.

The worker is constrained to `max_concurrent_activities=1` to force a backlog and make the ordering effects visible.
