from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.long_running_task import LongRunningTaskInput, run_long_task


@workflow.defn
class ConcurrencyDemoWorkflow:
    @workflow.run
    async def run(self, input: LongRunningTaskInput) -> str:
        return await workflow.execute_activity(
            run_long_task,
            input,
            start_to_close_timeout=timedelta(
                seconds=max(30, input.duration_seconds + 15),
            ),
        )
