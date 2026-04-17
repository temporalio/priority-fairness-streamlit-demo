from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.process_order import process_order


@dataclass
class ProcessOrderInput:
    order_id: str
    tenant: str
    priority: int


@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, input: ProcessOrderInput) -> str:
        return await workflow.execute_activity(
            process_order,
            input,
            start_to_close_timeout=timedelta(seconds=30),
        )
