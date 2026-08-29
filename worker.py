import asyncio
import logging

from temporalio.worker import Worker

from activities.handle_chat_turn import handle_chat_turn
from demo_config import DEMO_ADDRESS, connect_local_client
from workflows.chat_turn_workflow import ChatTurnWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    client = await connect_local_client(identity="priority-fairness-worker")
    logger.info("Connecting priority/fairness worker to %s", DEMO_ADDRESS)

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
