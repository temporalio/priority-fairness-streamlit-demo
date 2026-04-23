import asyncio
import random

from temporalio.client import Client
from temporalio.common import Priority
from temporalio.envconfig import ClientConfig

from activities.handle_chat_turn import ChatTurnInput
from workflows.chat_turn_workflow import ChatTurnWorkflow


async def main():
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    async def start_one(i):
        tier = random.choice([1, 3, 5])
        await client.start_workflow(
            ChatTurnWorkflow.run,
            ChatTurnInput(f"turn-{i:03d}", f"customer-{i}", tier),
            id=f"chat-turn-{i:03d}",
            task_queue="priority-fairness-task-queue",
            priority=Priority(priority_key=tier),
        )
        print(f"Started chat-turn-{i:03d} tier={tier}")

    await asyncio.gather(*[start_one(i) for i in range(50)])
    print("\nAll 50 chat turns started. Watch the worker logs for execution order.")


if __name__ == "__main__":
    asyncio.run(main())
