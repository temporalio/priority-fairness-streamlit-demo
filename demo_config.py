import json
import os
from urllib import error, parse, request

from temporalio.client import Client

DEMO_ADDRESS = os.getenv("TEMPORAL_DEMO_ADDRESS", "127.0.0.1:7233")
DEMO_HTTP_ADDRESS = os.getenv("TEMPORAL_DEMO_HTTP_ADDRESS", "http://127.0.0.1:7243")
DEMO_NAMESPACE = os.getenv("TEMPORAL_DEMO_NAMESPACE", "default")
CONCURRENCY_TASK_QUEUE = "concurrency-demo-task-queue"
CONCURRENCY_FAIRNESS_TASK_QUEUE = "concurrency-fairness-demo-task-queue"
CONCURRENCY_WORKER_IDENTITIES = tuple(
    f"concurrency-worker-{index}" for index in range(1, 5)
)
CONCURRENCY_WORKER_SLOTS = 4


async def connect_local_client(identity: str | None = None) -> Client:
    return await Client.connect(
        DEMO_ADDRESS,
        namespace=DEMO_NAMESPACE,
        identity=identity,
    )


def update_activity_task_queue_controls(
    *,
    task_queue: str,
    requests_per_second: float,
    concurrency_limit: int | None,
) -> dict:
    namespace = parse.quote(DEMO_NAMESPACE, safe="")
    task_queue_path = parse.quote(task_queue, safe="")
    url = (
        f"{DEMO_HTTP_ADDRESS.rstrip('/')}"
        f"/api/v1/namespaces/{namespace}/task-queues/{task_queue_path}/update-config"
    )
    concurrency_update: dict[str, object] = {
        "reason": "priority-fairness Streamlit concurrency demo",
    }
    if concurrency_limit is not None:
        concurrency_update["concurrencyLimit"] = {
            "concurrentTasks": concurrency_limit,
        }

    payload = {
        "identity": "concurrency-demo-dashboard",
        "taskQueueType": "TASK_QUEUE_TYPE_ACTIVITY",
        "updateQueueRateLimit": {
            "rateLimit": {"requestsPerSecond": requests_per_second},
            "reason": "priority-fairness Streamlit concurrency demo",
        },
        "updateQueueConcurrencyLimit": concurrency_update,
    }
    encoded_payload = json.dumps(payload).encode("utf-8")
    http_request = request.Request(
        url,
        data=encoded_payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(http_request, timeout=10) as response:
            return json.load(response)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Temporal returned HTTP {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(
            f"Could not reach Temporal HTTP API at {DEMO_HTTP_ADDRESS}: {exc.reason}"
        ) from exc


def get_activity_task_queue_config(task_queue: str) -> dict:
    namespace = parse.quote(DEMO_NAMESPACE, safe="")
    task_queue_path = parse.quote(task_queue, safe="")
    query = parse.urlencode(
        {
            "taskQueueType": "TASK_QUEUE_TYPE_ACTIVITY",
            "reportConfig": "true",
        }
    )
    url = (
        f"{DEMO_HTTP_ADDRESS.rstrip('/')}"
        f"/api/v1/namespaces/{namespace}/task-queues/{task_queue_path}?{query}"
    )
    try:
        with request.urlopen(url, timeout=10) as response:
            return json.load(response)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Temporal returned HTTP {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(
            f"Could not reach Temporal HTTP API at {DEMO_HTTP_ADDRESS}: {exc.reason}"
        ) from exc
