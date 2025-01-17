import asyncio
import json
import logging
from textwrap import dedent

logging.basicConfig(level=logging.INFO)


class Client:
    def __init__(self, coord_host: str, coord_port: int):
        self.coord_host = coord_host
        self.coord_port = coord_port

    async def submit_task(self, func_source: str, func_name: str, args: tuple, kwargs: dict) -> str:
        """
        Отправить задачу на выполнение координатору.

        :param func_source: Исходный код функции в виде строки.
        :param func_name: Имя функции для вызова.
        :param args: Аргументы функции.
        :param kwargs: Именованные аргументы функции.
        :return: Идентификатор задачи, если задача была успешно отправлена.
        """
        reader, writer = await asyncio.open_connection(self.coord_host, self.coord_port)

        func_source = dedent(func_source).strip()
        task_message = (json.dumps({
            "type": "submit_task",
            "func_source": func_source,
            "func_name": func_name,
            "args": args,
            "kwargs": kwargs,
        }) + "\n").encode()

        writer.write(task_message)
        await writer.drain()

        response = await reader.readline()
        response_data = json.loads(response.decode())

        writer.close()
        await writer.wait_closed()

        if response_data["type"] == "task_accepted":
            return response_data["task_id"]
        else:
            logging.error(f"Task submission failed: {response_data.get('reason', 'Unknown error')}")
            return None

    async def submit_task_map(self, func_source: str, func_name: str, args: tuple, kwargs: dict) -> str:
        """
        Отправить задачу с типом map на выполнение координатору, данные будут распределены между воркерами.

        :param func_source: Исходный код функции в виде строки.
        :param func_name: Имя функции для вызова.
        :param args: Коллекция элементов, которые нужно обработать параллельно.
        :param kwargs: Именованные аргументы функции.
        :return: Идентификатор задачи, если задача была успешно отправлена.
        """
        reader, writer = await asyncio.open_connection(self.coord_host, self.coord_port)

        func_source = dedent(func_source).strip()
        task_message = (json.dumps({
            "type": "submit_task_map",
            "func_source": func_source,
            "func_name": func_name,
            "args": args,
            "kwargs": kwargs,
        }) + "\n").encode()

        writer.write(task_message)
        await writer.drain()

        response = await reader.readline()
        response_data = json.loads(response.decode())

        writer.close()
        await writer.wait_closed()

        if response_data["type"] == "task_accepted":
            return response_data["task_id"]
        else:
            logging.error(f"Task submission failed: {response_data.get('reason', 'Unknown error')}")
            return None

    async def cancel_task(self, task_id: str) -> bool:
        """
        Отправить запрос на отмену задачи координатору.

        :param task_id: Идентификатор задачи для отмены.
        :return: Успешность отмены задачи.
        """
        reader, writer = await asyncio.open_connection(self.coord_host, self.coord_port)

        cancel_message = (json.dumps({
            "type": "cancel_task",
            "task_id": task_id,
        }) + "\n").encode()

        writer.write(cancel_message)
        await writer.drain()

        response = await reader.readline()
        response_data = json.loads(response.decode())

        writer.close()
        await writer.wait_closed()

        if response_data.get("status") == "cancelled":
            logging.info(f"Task {task_id} cancelled successfully.")
            return True
        else:
            logging.warning(f"Failed to cancel task {task_id}. Reason: {response_data.get('reason', 'Unknown error')}")
            return False

    async def get_task_status(self, task_id: str):
        """
        Проверить статус задачи.

        :param task_id: Идентификатор задачи.
        :return: Статус задачи в виде словаря.
        """
        reader, writer = await asyncio.open_connection(self.coord_host, self.coord_port)

        status_request = (json.dumps({
            "type": "get_task_status",
            "task_id": task_id,
        }) + "\n").encode()

        writer.write(status_request)
        await writer.drain()

        response = await reader.readline()
        response_data = json.loads(response.decode())

        writer.close()
        await writer.wait_closed()

        return response_data.get("task", {"status": "not found"})


if __name__ == "__main__":
    coord_host = "localhost"
    coord_port = 8765

    client = Client(coord_host, coord_port)

    async def main():
        # Пример задачи: передаем функцию сложения чисел
        func_source = """
        def compute_fibonacci_sum_of_squares(n):
            def fibonacci(x):
                if x <= 1:
                    return x
                return fibonacci(x - 1) + fibonacci(x - 2)

            total = 0
            for i in range(n):
                fib = fibonacci(i)
                total += fib ** 2
            return total
        """
        func_sq = """
        def sq(n):
            return n * n
        """
        func_name = "compute_fibonacci_sum_of_squares"
        func_name_sq = "sq"
        args = (1,)
        kwargs = {}

        #task_id = await client.submit_task(func_source, func_name, args, kwargs)
        task_id = await client.submit_task_map(func_sq, func_name_sq, (1, 2, 3, 4, 5), kwargs)
        while True:
            status = await client.get_task_status(task_id)
            logging.info(f"Task {task_id} status: {status['status']}")
            if status['status'] == "completed":
                logging.info(f"Task {task_id} completed successfully. Result: {status['result']}")
                break
            elif status['status'] == "in progress":
                #await client.cancel_task(task_id)
                pass
            else:
                logging.error(f"Task {task_id} failed with status: {status['status']}")
                break
            await asyncio.sleep(1)


    asyncio.run(main())
