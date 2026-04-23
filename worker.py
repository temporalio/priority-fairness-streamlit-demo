import asyncio
import logging

from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from activities.handle_chat_turn import handle_chat_turn
from workflows.chat_turn_workflow import ChatTurnWorkflow

logging.basicConfig(level=logging.INFO)


async def main():
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    worker = Worker(
        client,
        task_queue="priority-fairness-task-queue",
        workflows=[ChatTurnWorkflow],
        activities=[handle_chat_turn],
        max_concurrent_activities=1,
        disable_eager_activity_execution=True,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
