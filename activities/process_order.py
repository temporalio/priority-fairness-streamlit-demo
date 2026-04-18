import asyncio
from dataclasses import dataclass

from temporalio import activity


@dataclass
class ProcessOrderInput:
    order_id: str
    tenant: str
    priority: int


@activity.defn
async def process_order(input: ProcessOrderInput) -> str:
    activity.logger.info(
        f"Executing order {input.order_id} (priority {input.priority}, tenant {input.tenant})"
    )
    await asyncio.sleep(0.5)
    return f"Order {input.order_id} processed (priority {input.priority})"
