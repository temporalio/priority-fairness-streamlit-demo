import asyncio
import logging
from datetime import timedelta

from temporalio.api.workflowservice.v1 import RegisterNamespaceRequest
from temporalio.service import RPCError, RPCStatusCode

from demo_config import DEMO_ADDRESS, DEMO_NAMESPACE, connect_local_client

logger = logging.getLogger(__name__)


async def main() -> None:
    deadline = asyncio.get_running_loop().time() + 120
    while True:
        try:
            client = await connect_local_client(identity="concurrency-demo-bootstrap")
            await client.workflow_service.register_namespace(
                RegisterNamespaceRequest(
                    namespace=DEMO_NAMESPACE,
                    workflow_execution_retention_period=timedelta(days=1),
                )
            )
            logger.info("Created namespace %s", DEMO_NAMESPACE)
            return
        except RPCError as exc:
            if exc.status == RPCStatusCode.ALREADY_EXISTS:
                logger.info("Namespace %s already exists", DEMO_NAMESPACE)
                return
            last_error = exc
        except (ConnectionError, OSError, RuntimeError) as exc:
            last_error = exc

        if asyncio.get_running_loop().time() >= deadline:
            raise RuntimeError(
                f"Temporal at {DEMO_ADDRESS} did not become ready"
            ) from last_error
        await asyncio.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
