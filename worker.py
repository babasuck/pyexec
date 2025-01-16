import asyncio
import json
import logging

logging.basicConfig(level=logging.INFO)

class Worker:
    def __init__(self, coord_host, coord_port):
        self.coord_host = coord_host
        self.coord_port = coord_port
        self.worker_id = None
        self.current_task = None
        self.task_status = {}  # Хранение статусов задач: {task_id: status}

    async def connect_to_coord(self):
        logging.info(f"Connecting to coordinator at {self.coord_host}:{self.coord_port}")
        reader, writer = await asyncio.open_connection(self.coord_host, self.coord_port)
        await self.register_worker(writer)
        await self.listen_for_tasks(reader, writer)

    async def register_worker(self, writer):
        logging.info("Registering worker with coordinator...")
        writer.write((json.dumps({"type": "register_worker"}) + "\n").encode())
        await writer.drain()
        logging.info("Worker registered successfully.")

    async def listen_for_tasks(self, reader, writer):
        logging.info("Listening for tasks from coordinator...")
        try:
            while True:
                message = await reader.readline()
                if not message:
                    logging.warning("Connection to coordinator lost.")
                    break

                data = json.loads(message.decode())
                if data["type"] == "task":
                    logging.info(f"Received task: {data['task_id']}")
                    await self.handle_task(data, writer)
                elif data["type"] == "cancel_task":
                    logging.info(f"Received cancel request for task: {data['task_id']}")
                    await self.cancel_task(data)
        except asyncio.CancelledError:
            logging.info("Worker cancelled.")
        except Exception as e:
            logging.error(f"Error while listening for tasks: {e}")
        finally:
            logging.info("Connection to coordinator closed.")
            writer.close()
            await writer.wait_closed()

    async def handle_task(self, task_message, writer):
        task_id = task_message["task_id"]
        func_source = task_message["func_source"]
        func_name = task_message["func_name"]
        args = task_message["args"]
        kwargs = task_message["kwargs"]

        self.current_task = task_id
        self.task_status[task_id] = "in progress"

        try:

            result = await asyncio.get_event_loop().run_in_executor(None, self.execute_code, func_source,
                                                                    func_name,
                                                                    args,
                                                                    kwargs)

            result_message = json.dumps({
                "type": "task_result",
                "task_id": task_id,
                "status": "completed",
                "result": result,
            }) + "\n"
            logging.info(f"Task {task_id} completed successfully.")
            self.task_status[task_id] = "completed"

        except Exception as e:
            result_message = json.dumps({
                "type": "task_result",
                "task_id": task_id,
                "status": "failed",
                "result": str(e),
            }) + "\n"
            logging.error(f"Task {task_id} failed with error: {e}")
            self.task_status[task_id] = "failed"
        finally:
            writer.write(result_message.encode())
            await writer.drain()
            self.current_task = None

    def execute_code(self, func_source, func_name, args, kwargs):
        """
        Функция для выполнения кода в отдельном потоке.
        """
        try:
            exec(func_source, globals())
            func = globals().get(func_name)

            if func is None:
                raise ValueError(f"Function '{func_name}' not found in provided code.")

            return func(*args, **kwargs)

        except Exception as e:
            raise e

    async def cancel_task(self, cancel_message):
        task_id = cancel_message["task_id"]
        if task_id not in self.task_status:
            logging.info(f"No matching task to cancel: {task_id}")
            return

        task_status = self.task_status[task_id]
        if task_status == "in progress":
            logging.info(f"Task {task_id} cancelled by coordinator.")
            self.task_status[task_id] = "cancelled"
            self.current_task = None
        else:
            logging.info(f"Task {task_id} is already {task_status}, cannot cancel.")


if __name__ == "__main__":
    worker = Worker("localhost", 8765)
    asyncio.run(worker.connect_to_coord())
