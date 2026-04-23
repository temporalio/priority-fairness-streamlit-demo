from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities.handle_chat_turn import ChatTurnInput, handle_chat_turn


@workflow.defn
class ChatTurnWorkflow:
    @workflow.run
    async def run(self, input: ChatTurnInput) -> str:
        return await workflow.execute_activity(
            handle_chat_turn,
            input,
            start_to_close_timeout=timedelta(seconds=30),
        )
