import asyncio
import logging

from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from activities.process_order import process_order
from workflows.order_workflow import OrderWorkflow

logging.basicConfig(level=logging.INFO)


async def main():
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    worker = Worker(
        client,
        task_queue="priority-fairness-task-queue",
        workflows=[OrderWorkflow],
        activities=[process_order],
        max_concurrent_activities=1,
        disable_eager_activity_execution=True,
    )
    print("Worker started (max_concurrent_activities=1, eager dispatch disabled)")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
