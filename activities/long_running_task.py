import asyncio
from dataclasses import dataclass

from temporalio import activity


@dataclass
class LongRunningTaskInput:
    task_id: str
    customer_id: str
    duration_seconds: float


@activity.defn
async def run_long_task(input: LongRunningTaskInput) -> str:
    activity.logger.info(
        "Running %s for %s for %.1fs",
        input.task_id,
        input.customer_id,
        input.duration_seconds,
    )
    await asyncio.sleep(input.duration_seconds)
    return input.task_id
