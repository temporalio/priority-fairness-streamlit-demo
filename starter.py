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

    target_host = config["target_host"]
    if "localhost" in target_host or "127.0.0.1" in target_host:
        ui_base = "http://localhost:8233"
    else:
        ui_base = "https://cloud.temporal.io"

    async def start_one(i):
        tier = random.choice([1, 3, 5])
        handle = await client.start_workflow(
            ChatTurnWorkflow.run,
            ChatTurnInput(f"turn-{i:03d}", f"customer-{i}", tier),
            id=f"chat-turn-{i:03d}",
            task_queue="priority-fairness-task-queue",
            priority=Priority(priority_key=tier),
        )
        url = f"{ui_base}/namespaces/{client.namespace}/workflows/{handle.id}/{handle.result_run_id}"
        print(f"Started chat-turn-{i:03d} tier={tier} {url}")

    await asyncio.gather(*[start_one(i) for i in range(10)])
    print("\nAll 10 chat turns started. Watch the worker logs for execution order.")


if __name__ == "__main__":
    asyncio.run(main())
