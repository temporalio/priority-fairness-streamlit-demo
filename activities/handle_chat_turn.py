import asyncio
from dataclasses import dataclass

from temporalio import activity


@dataclass
class ChatTurnInput:
    turn_id: str
    customer_id: str
    tier: int


@activity.defn
async def handle_chat_turn(input: ChatTurnInput) -> str:
    activity.logger.info(
        f"Handling chat turn {input.turn_id} for customer {input.customer_id} (tier {input.tier})"
    )
    await asyncio.sleep(0.5)
    return f"Chat turn {input.turn_id} completed (customer {input.customer_id}, tier {input.tier})"
