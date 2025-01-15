import pickle
import asyncio
import uuid
from typing import Any, Dict


class Worker:
    def __init__(self, name: str) -> None:
        self.name = name
        self.loaded_code: set[str] = set()

    def execute_code(self, func_source: str, args: tuple, kwargs: dict) -> Any:
        local_scope: Dict[str, Any] = {}
        exec(func_source, globals(), local_scope)
        func = next(iter(local_scope.values()))
        return func(*args, **kwargs)


class TaskDispatcher:
    def __init__(self) -> None:
        self.workers: Dict[str, Worker] = {}

    def register_worker(self, worker: Worker) -> None:
        self.workers[worker.name] = worker

    def find_worker(self, func_source: str) -> Worker:
        for worker in self.workers.values():
            if func_source in worker.loaded_code:
                return worker
        return next(iter(self.workers.values()))


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, dispatcher: TaskDispatcher) -> None:
    data = await reader.read(4096)
    func_source, args, kwargs = pickle.loads(data)
    task_id = str(uuid.uuid4())
    worker = dispatcher.find_worker(func_source)

    async def send_status(status: str, result: Any = None) -> None:
        status_data = {"task_id": task_id, "status": status, "result": result}
        writer.write(pickle.dumps(status_data))
        await writer.drain()

    await send_status("started")
    loop = asyncio.get_running_loop()

    try:
        result = await loop.run_in_executor(
            None, worker.execute_code, func_source, args, kwargs
        )
        await send_status("in progress")
        await asyncio.sleep(0.1)  # Имитируем прогресс
        await send_status("completed", result)
    except Exception as e:
        await send_status("failed", str(e))

    writer.close()


async def server() -> None:
    dispatcher = TaskDispatcher()
    workers = [Worker(f"worker_{i}") for i in range(3)]
    for worker in workers:
        dispatcher.register_worker(worker)

    srv = await asyncio.start_server(
        lambda r, w: handle_client(r, w, dispatcher), 'localhost', 12345
    )
    addr = srv.sockets[0].getsockname()
    print(f"Сервер запущен на {addr}")

    async with srv:
        await srv.serve_forever()


if __name__ == '__main__':
    asyncio.run(server())
