import argparse
import asyncio
import logging
import sys
from pathlib import Path

from temporalio.worker import Worker

from activities.long_running_task import run_long_task
from demo_config import (
    CONCURRENCY_FAIRNESS_TASK_QUEUE,
    CONCURRENCY_TASK_QUEUE,
    CONCURRENCY_WORKER_IDENTITIES,
    CONCURRENCY_WORKER_SLOTS,
    DEMO_ADDRESS,
    connect_local_client,
)
from workflows.concurrency_workflow import ConcurrencyDemoWorkflow

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run multiple workers for the task queue concurrency demo.",
    )
    parser.add_argument("--slots", type=int, default=CONCURRENCY_WORKER_SLOTS)
    parser.add_argument("--worker-id", help=argparse.SUPPRESS)
    parser.add_argument("--task-queue", help=argparse.SUPPRESS)
    return parser.parse_args()


async def run_worker(worker_id: str, slots: int, task_queue: str) -> None:
    client = await connect_local_client(identity=worker_id)
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[ConcurrencyDemoWorkflow],
        activities=[run_long_task],
        identity=worker_id,
        max_concurrent_activities=slots,
        disable_eager_activity_execution=True,
    )
    logger.info(
        "Starting %s with %d activity slots on %s (%s)",
        worker_id,
        slots,
        task_queue,
        DEMO_ADDRESS,
    )
    await worker.run()


async def run_launcher(worker_identities: tuple[str, ...], slots: int) -> None:
    task_queues = (
        CONCURRENCY_TASK_QUEUE,
        CONCURRENCY_FAIRNESS_TASK_QUEUE,
    )
    logger.info(
        "Starting %d logical workers per task queue with %d activity slots each on %s (%s)",
        len(worker_identities),
        slots,
        ", ".join(task_queues),
        DEMO_ADDRESS,
    )
    script = Path(__file__).resolve()
    processes = [
        await asyncio.create_subprocess_exec(
            sys.executable,
            str(script),
            "--worker-id",
            worker_id,
            "--task-queue",
            task_queue,
            "--slots",
            str(slots),
        )
        for task_queue in task_queues
        for worker_id in worker_identities
    ]
    try:
        return_codes = await asyncio.gather(*(process.wait() for process in processes))
        if any(return_code != 0 for return_code in return_codes):
            raise RuntimeError(f"worker processes exited with {return_codes}")
    finally:
        for process in processes:
            if process.returncode is None:
                process.terminate()
        await asyncio.gather(*(process.wait() for process in processes))


async def main() -> None:
    args = parse_args()
    if args.slots < 1:
        raise ValueError("--slots must be at least 1")
    if args.worker_id:
        if not args.task_queue:
            raise ValueError("--task-queue is required with --worker-id")
        await run_worker(args.worker_id, args.slots, args.task_queue)
    else:
        await run_launcher(CONCURRENCY_WORKER_IDENTITIES, args.slots)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
