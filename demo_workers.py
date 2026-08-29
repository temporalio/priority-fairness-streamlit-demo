import asyncio
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


async def main() -> None:
    project_dir = Path(__file__).resolve().parent
    commands = [
        ("priority/Fairness worker", project_dir / "worker.py"),
        ("concurrency worker fleet", project_dir / "concurrency_worker.py"),
    ]
    processes = []
    for label, script in commands:
        logger.info("Starting %s", label)
        processes.append(
            await asyncio.create_subprocess_exec(sys.executable, str(script))
        )

    try:
        return_codes = await asyncio.gather(
            *(process.wait() for process in processes)
        )
        if any(return_code != 0 for return_code in return_codes):
            raise RuntimeError(f"worker processes exited with {return_codes}")
    finally:
        for process in processes:
            if process.returncode is None:
                process.terminate()
        await asyncio.gather(*(process.wait() for process in processes))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
