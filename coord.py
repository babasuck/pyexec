import asyncio
import json
import logging
import uuid

logging.basicConfig(level=logging.INFO)


class Coordinator:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.workers = {}  # {worker_id: (reader, writer, code_info)}
        self.clients = {}
        self.tasks = {}

    async def start_server(self):
        server = await asyncio.start_server(self.handle_connection, self.host, self.port)
        addr = server.sockets[0].getsockname()
        logging.info(f"Coordinator running on {addr}")
        async with server:
            await server.serve_forever()

    async def handle_connection(self, reader, writer):
        client_id = str(uuid.uuid4())
        worker_id = None
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break

                message = json.loads(data.decode())
                if message["type"] == "register_worker":
                    worker_id = await self.register_worker(reader, writer, message)
                elif message["type"] == "submit_task":
                    await self.submit_task(message, reader, writer, client_id)
                elif message["type"] == "submit_task_map":
                    await self.submit_task_map(message, reader, writer, client_id)
                elif message["type"] == "cancel_task":
                    await self.cancel_task(message, writer)
                elif message["type"] == "get_task_status":
                    await self.get_task_status(message, writer)
                elif message["type"] == "task_result":
                    await self.handle_task_result(message)

        except Exception as e:
            logging.error(f"Error handling connection: {e}")
        finally:
            if worker_id:
                await self.unregister_worker(worker_id)
            elif client_id in self.clients:
                self.clients.pop(client_id, None)

    async def register_worker(self, reader, writer, message):
        worker_id = str(uuid.uuid4())
        # Регистрация воркера с его кодом
        code_info = message.get("code_info", [])  # Массив с информацией о кодах
        self.workers[worker_id] = (reader, writer, code_info)
        writer.write((json.dumps({"type": "register_success", "worker_id": worker_id}) + "\n").encode())
        await writer.drain()
        logging.info(f"Worker registered with ID: {worker_id}")
        return worker_id

    async def unregister_worker(self, worker_id):
        if worker_id in self.workers:
            for task_id, task in self.tasks.items():
                if task["worker_id"] == worker_id and task["status"] == "in progress":
                    task["status"] = "failed"
                    task["result"] = "Worker disconnected."
            del self.workers[worker_id]
            logging.info(f"Worker {worker_id} disconnected.")

    async def submit_task(self, message, reader, writer, client_id):
        if not self.workers:
            writer.write((json.dumps({"type": "task_rejected", "reason": "No workers available"}) + "\n").encode())
            await writer.drain()
            return

        task_id = str(uuid.uuid4())
        func_name = message["func_name"]
        worker_id = None
        worker_found = False

        # Проверка, на каком воркере уже есть нужный код
        for worker, (worker_reader, worker_writer, code_info) in self.workers.items():
            if func_name in code_info:
                worker_id = worker
                worker_found = True
                break

        if not worker_found:
            # Если нужный код не найден, назначаем задачу любому доступному воркеру
            worker_id, (worker_reader, worker_writer, code_info) = next(iter(self.workers.items()))
            # Передаем код воркеру, если у него его нет
            self.workers[worker_id] = (worker_reader, worker_writer, code_info + [func_name])
            logging.info(f"Worker {worker_id} now supports the function '{func_name}'.")

        self.tasks[task_id] = {"worker_id": worker_id, "status": "in progress", "result": None}
        self.clients[task_id] = (reader, writer)

        task_message = (json.dumps({
            "type": "task",
            "task_id": task_id,
            "func_source": message["func_source"],
            "func_name": func_name,
            "args": message["args"],
            "kwargs": message["kwargs"],
        }) + "\n").encode()
        worker_writer = self.workers[worker_id][1]
        worker_writer.write(task_message)
        await worker_writer.drain()

        writer.write((json.dumps({"type": "task_accepted", "task_id": task_id}) + "\n").encode())
        await writer.drain()

    async def submit_task_map(self, message, reader, writer, client_id):
        """
        Обработка задачи с распределением элементов коллекции на несколько воркеров.
        """
        if not self.workers:
            writer.write((json.dumps({"type": "task_rejected", "reason": "No workers available"}) + "\n").encode())
            await writer.drain()
            return

        task_id = str(uuid.uuid4())
        func_name = message["func_name"]
        args = message["args"]
        num_chunks = len(self.workers)
        chunk_size = len(args) // num_chunks
        chunks = [args[i:i + chunk_size] for i in range(0, len(args), chunk_size)]

        # Разделяем задачу на части и отправляем каждому воркеру
        results = []
        for i, chunk in enumerate(chunks):
            worker_id, (worker_reader, worker_writer, code_info) = list(self.workers.items())[i]
            task_message = (json.dumps({
                "type": "task",
                "task_id": task_id,
                "func_source": message["func_source"],
                "func_name": func_name,
                "args": chunk,
                "kwargs": message["kwargs"],
            }) + "\n").encode()
            worker_writer.write(task_message)
            await worker_writer.drain()

            result = await self.collect_result(task_id)
            results.append(result)

        # Объединяем результаты и отправляем обратно клиенту
        final_result = sum(results)  # Пример для числовых данных
        writer.write((json.dumps({
            "type": "task_result",
            "task_id": task_id,
            "status": "completed",
            "result": final_result
        }) + "\n").encode())
        await writer.drain()

    async def collect_result(self, task_id):
        """
        Сбор результата от воркера.
        """
        result = None
        if task_id in self.tasks:
            result = self.tasks[task_id]["result"]
        return result if result is not None else 0

    async def cancel_task(self, message, writer):
        task_id = message["task_id"]
        if task_id in self.tasks and self.tasks[task_id]["status"] == "in progress":
            worker_id = self.tasks[task_id]["worker_id"]
            worker_writer = self.workers[worker_id][1]
            cancel_message = (json.dumps({"type": "cancel_task", "task_id": task_id}) + "\n").encode()
            worker_writer.write(cancel_message)
            await worker_writer.drain()

            self.tasks[task_id]["status"] = "cancelled"
            writer.write((json.dumps({"type": "task_cancelled", "task_id": task_id, "status": "cancelled"}) + "\n").encode())
        else:
            writer.write((json.dumps({"type": "task_not_found", "task_id": task_id}) + "\n").encode())
        await writer.drain()

    async def get_task_status(self, message, writer):
        task_id = message["task_id"]
        if task_id in self.tasks:
            writer.write((json.dumps({"type": "task_status", "task": self.tasks[task_id]}) + "\n").encode())
        else:
            writer.write((json.dumps({"type": "task_not_found"}) + "\n").encode())
        await writer.drain()

    async def handle_task_result(self, message):
        task_id = message["task_id"]
        if task_id in self.tasks:
            self.tasks[task_id]["status"] = message["status"]
            self.tasks[task_id]["result"] = message["result"]
            if task_id in self.clients:
                client_reader, client_writer = self.clients.pop(task_id)
                client_writer.write((json.dumps(message) + "\n").encode())
                await client_writer.drain()


if __name__ == "__main__":
    coordinator = Coordinator("localhost", 8765)
    asyncio.run(coordinator.start_server())
