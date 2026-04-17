import asyncio
import random

from temporalio.client import Client
from temporalio.common import Priority
from temporalio.envconfig import ClientConfig

from workflows.order_workflow import OrderWorkflow, ProcessOrderInput


def workflow_url(client: Client, handle) -> str:
    return f"https://cloud.temporal.io/namespaces/{client.namespace}/workflows/{handle.id}/{handle.result_run_id}/timeline"


async def main():
    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    # Build a batch of workflows with mixed priorities
    orders = []
    for i in range(50):
        priority = random.choice([1, 3, 5])
        orders.append((f"ORD-{i:03d}", f"tenant-{i}", priority))

    # Start all workflows concurrently
    async def start_one(order_id, tenant, priority):
        handle = await client.start_workflow(
            OrderWorkflow.run,
            ProcessOrderInput(order_id, tenant, priority),
            id=f"order-{order_id}",
            task_queue="priority-fairness-task-queue",
            priority=Priority(priority_key=priority),
        )
        print(f"Started {order_id} priority={priority}")
        return handle, priority

    results = await asyncio.gather(
        *[start_one(oid, t, p) for oid, t, p in orders]
    )
    handles = list(results)

    print(f"\nAll {len(handles)} workflows started. Waiting for results...\n")

    # Collect results as they complete (not in submission order)
    async def wait_one(handle, priority):
        result = await handle.result()
        return result

    done_order = []
    pending = {
        asyncio.create_task(wait_one(h, p)): (h, p) for h, p in handles
    }
    position = 1
    while pending:
        done, _ = await asyncio.wait(pending.keys(), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            result = task.result()
            print(f"  [{position:2d}] {result}")
            position += 1
            del pending[task]


if __name__ == "__main__":
    asyncio.run(main())
