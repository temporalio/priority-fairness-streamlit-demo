import asyncio
import random

from temporalio.client import Client
from temporalio.common import Priority
from temporalio.envconfig import ClientConfig

from workflows.order_workflow import OrderWorkflow, ProcessOrderInput


async def main():
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    # Start 50 workflows concurrently with mixed priorities
    async def start_one(i):
        priority = random.choice([1, 3, 5])
        await client.start_workflow(
            OrderWorkflow.run,
            ProcessOrderInput(f"ORD-{i:03d}", f"tenant-{i}", priority),
            id=f"order-ORD-{i:03d}",
            task_queue="priority-fairness-task-queue",
            priority=Priority(priority_key=priority),
        )
        print(f"Started ORD-{i:03d} priority={priority}")

    await asyncio.gather(*[start_one(i) for i in range(50)])
    print("\nAll 50 workflows started. Watch the worker logs for execution order.")


if __name__ == "__main__":
    asyncio.run(main())
