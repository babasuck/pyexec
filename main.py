import asyncio
import logging
from client import Client
from coord import Coordinator
from worker import Worker

logging.basicConfig(level=logging.INFO)

async def main():
    coord_host = "localhost"
    coord_port = 8765
    coordinator = Coordinator(coord_host, coord_port)
    await asyncio.sleep(2)
    worker = Worker(coord_host, coord_port)
    await asyncio.sleep(2)
    client = Client(coord_host, coord_port)

    coord_task = asyncio.create_task(coordinator.start_server())
    worker_task = asyncio.create_task(worker.connect_to_coord())


    func_source = """
    def add(x, y):
        return x + y
    """
    func_name = "add"
    args = (1, 2)
    kwargs = {}

    await asyncio.sleep(2)
    task_id = await client.submit_task(func_source, func_name, args, kwargs)
    status = await client.get_task_status(task_id)

    logging.info(f"Task {task_id} status: {status['status']}")

    if status["status"] == "completed":
        logging.info(f"Task {task_id} completed successfully. Result: {status['result']}")
    else:
        logging.error(f"Task {task_id} failed with status: {status['status']}")

    await asyncio.gather(coord_task, worker_task)

if __name__ == "__main__":
    asyncio.run(main())
