import asyncio
import os
import time
import urllib.request
import uuid

from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import ListTaskQueuePartitionsRequest
from temporalio.common import Priority

from activities.handle_chat_turn import ChatTurnInput
from activities.long_running_task import LongRunningTaskInput
from demo_config import (
    CONCURRENCY_FAIRNESS_TASK_QUEUE,
    CONCURRENCY_TASK_QUEUE,
    connect_local_client,
    get_activity_task_queue_config,
    update_activity_task_queue_controls,
)
from workflows.chat_turn_workflow import ChatTurnWorkflow
from workflows.concurrency_workflow import ConcurrencyDemoWorkflow

PRIORITY_FAIRNESS_TASK_QUEUE = "priority-fairness-task-queue"


async def run_priority_and_fairness_smoke(client) -> None:
    run_id = uuid.uuid4().hex[:10]
    priority_handles = [
        await client.start_workflow(
            ChatTurnWorkflow.run,
            ChatTurnInput(
                turn_id=f"priority-{index}",
                customer_id=f"customer-{index}",
                tier=priority_key,
            ),
            id=f"e2e-priority-{run_id}-{index}",
            task_queue=PRIORITY_FAIRNESS_TASK_QUEUE,
            priority=Priority(priority_key=priority_key),
        )
        for index, priority_key in enumerate((5, 3, 1))
    ]
    await asyncio.gather(*(handle.result() for handle in priority_handles))

    fairness_handles = [
        await client.start_workflow(
            ChatTurnWorkflow.run,
            ChatTurnInput(
                turn_id=f"fairness-{index}",
                customer_id=customer,
                tier=1,
            ),
            id=f"e2e-fairness-{run_id}-{index}",
            task_queue=PRIORITY_FAIRNESS_TASK_QUEUE,
            priority=Priority(fairness_key=customer),
        )
        for index, customer in enumerate(("bigcorp", "midco", "startup"))
    ]
    await asyncio.gather(*(handle.result() for handle in fairness_handles))
    print("PASS priority and Fairness workflows completed")


async def wait_for_partitions(client, task_queue: str, expected: int) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        response = await client.workflow_service.list_task_queue_partitions(
            ListTaskQueuePartitionsRequest(
                namespace=client.namespace,
                task_queue=TaskQueue(name=task_queue),
            )
        )
        if len(response.activity_task_queue_partitions) == expected:
            return
        await asyncio.sleep(0.5)
    raise AssertionError(f"{task_queue} did not report {expected} activity partitions")


async def run_limited_batch(
    client,
    *,
    task_queue: str,
    concurrency_limit: int,
    use_fairness: bool,
) -> None:
    update_activity_task_queue_controls(
        task_queue=task_queue,
        requests_per_second=100.0,
        concurrency_limit=concurrency_limit,
    )
    await asyncio.sleep(2)
    config = get_activity_task_queue_config(task_queue)["config"]
    observed_limit = config["queueConcurrencyLimit"]["concurrencyLimit"][
        "concurrentTasks"
    ]
    if observed_limit != concurrency_limit:
        raise AssertionError(
            f"{task_queue} reported concurrency {observed_limit}, expected {concurrency_limit}"
        )

    run_id = uuid.uuid4().hex[:10]
    handles = []
    tenants = ["bigcorp", "midco", "startup"]
    for index in range(8):
        kwargs = {
            "id": f"e2e-{run_id}-{index}",
            "task_queue": task_queue,
        }
        if use_fairness:
            kwargs["priority"] = Priority(fairness_key=tenants[index % len(tenants)])
        handles.append(
            await client.start_workflow(
                ConcurrencyDemoWorkflow.run,
                LongRunningTaskInput(
                    task_id=f"e2e-{index}",
                    customer_id=tenants[index % len(tenants)],
                    duration_seconds=2.0,
                ),
                **kwargs,
            )
        )

    deadline = time.monotonic() + 60
    maximum_running = 0
    while time.monotonic() < deadline:
        descriptions = await asyncio.gather(*(handle.describe() for handle in handles))
        running = sum(
            bool(description.raw_description.pending_activities)
            and description.raw_description.pending_activities[0].state == 2
            for description in descriptions
        )
        maximum_running = max(maximum_running, running)
        if running > concurrency_limit:
            raise AssertionError(
                f"{task_queue} ran {running} activities with limit {concurrency_limit}"
            )
        if all("COMPLETED" in description.status.name for description in descriptions):
            break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError(f"{task_queue} batch did not finish")

    await asyncio.gather(*(handle.result() for handle in handles))
    if maximum_running != concurrency_limit:
        raise AssertionError(
            f"{task_queue} reached only {maximum_running} running activities; "
            f"expected {concurrency_limit}"
        )
    print(
        f"PASS {task_queue}: maximum running={maximum_running}, "
        f"limit={concurrency_limit}"
    )


async def main() -> None:
    health_url = os.environ.get(
        "STREAMLIT_HEALTH_URL",
        "http://127.0.0.1:8501/_stcore/health",
    )
    def check_streamlit_health() -> None:
        with urllib.request.urlopen(health_url, timeout=5) as response:
            if response.read().decode().strip() != "ok":
                raise AssertionError("Streamlit health check did not return ok")

    await asyncio.to_thread(check_streamlit_health)

    client = await connect_local_client(identity="concurrency-demo-e2e")
    await run_priority_and_fairness_smoke(client)
    await wait_for_partitions(client, CONCURRENCY_TASK_QUEUE, 4)
    await wait_for_partitions(client, CONCURRENCY_FAIRNESS_TASK_QUEUE, 1)
    print("PASS task queue partitions: concurrency=4, concurrency+Fairness=1")

    await run_limited_batch(
        client,
        task_queue=CONCURRENCY_TASK_QUEUE,
        concurrency_limit=2,
        use_fairness=False,
    )
    await run_limited_batch(
        client,
        task_queue=CONCURRENCY_FAIRNESS_TASK_QUEUE,
        concurrency_limit=3,
        use_fairness=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
