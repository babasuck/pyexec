import unittest
import asyncio
from client import Client
from coord import Coordinator
from worker import Worker


class TestSystemExtended(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.coord_host = "localhost"
        self.coord_port = 8765

        self.coordinator = Coordinator(self.coord_host, self.coord_port)
        self.worker = Worker(self.coord_host, self.coord_port)
        self.worker_2 = Worker(self.coord_host, self.coord_port)
        self.worker_3 = Worker(self.coord_host, self.coord_port)

        self.client = Client(self.coord_host, self.coord_port)

        self.coord_server = asyncio.create_task(self.coordinator.start_server())
        await asyncio.sleep(0.1)

        self.worker_task = asyncio.create_task(self.worker.connect_to_coord())
        await asyncio.sleep(0.1)

        #self.worker_task_2 = asyncio.create_task(self.worker_2.connect_to_coord())
        await asyncio.sleep(0.1)

        #self.worker_task_3 = asyncio.create_task(self.worker_3.connect_to_coord())
        await asyncio.sleep(0.1)

    async def asyncTearDown(self):
        # Завершение работы сервера и воркера
        self.coord_server.cancel()
        self.worker_task.cancel()

    async def test_submit_task_success(self):
        # Проверка успешной отправки задачи
        func_source = "def add(x, y): return x + y"
        args = (1, 2)
        kwargs = {}

        task_id = await self.client.submit_task(func_source, "add", args, kwargs)
        self.assertIsNotNone(task_id, "Task ID должен быть возвращён.")

    async def test_get_task_status_success(self):
        # Проверка успешного получения статуса задачи
        func_source = "def multiply(x, y): return x * y"
        args = (2, 3)
        kwargs = {}

        task_id = await self.client.submit_task(func_source, "multiply", args, kwargs)
        self.assertIsNotNone(task_id, "Task ID должен быть возвращён.")
        await asyncio.sleep(1)

        status = await self.client.get_task_status(task_id)
        self.assertEqual(status["status"], "completed", "Задача должна быть выполнена.")
        self.assertEqual(status["result"], 6, "Результат выполнения должен быть корректным.")

    async def test_worker_handle_invalid_code(self):
        # Проверка обработки некорректного кода воркером
        func_source = "def invalid_code(x, y): return x // "
        args = (5, 3)
        kwargs = {}

        task_id = await self.client.submit_task(func_source, "invalid_code", args, kwargs)
        await asyncio.sleep(1)

        status = await self.client.get_task_status(task_id)
        self.assertEqual(status["status"], "failed", "Задача должна завершиться с ошибкой.")
        self.assertIn("invalid syntax", status["result"], "Должно быть указано сообщение об ошибке.")

    async def test_no_workers_available(self):
        # Проверка отправки задачи без подключенных воркеров
        self.worker_task.cancel()  # Отключаем воркера
        await asyncio.sleep(0.1)

        func_source = "def add(x, y): return x + y"
        args = (1, 2)
        kwargs = {}

        task_id = await self.client.submit_task(func_source, "add", args, kwargs)
        self.assertIsNone(task_id, "Task ID должен быть None, если нет доступных воркеров.")

    async def test_task_not_found(self):
        # Проверка запроса статуса несуществующей задачи
        status = await self.client.get_task_status("nonexistent_task_id")
        self.assertEqual(status["status"], "not found", "Статус задачи должен быть 'not found'.")

    async def test_worker_disconnect_handling(self):
        # Проверка обработки отключения воркера
        self.worker_task.cancel()
        await asyncio.sleep(0.1)

        func_source = "def add(x, y): return x + y"
        args = (1, 2)
        kwargs = {}

        task_id = await self.client.submit_task(func_source, "add", args, kwargs)
        self.assertIsNone(task_id, "Задача не должна быть принята без активных воркеров.")

    async def test_mult_task_for_worker(self):
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
        args = (35,)
        kwargs = {}

        func_name = "compute_fibonacci_sum_of_squares"

        task_id_1 = await self.client.submit_task(func_source, func_name, args, kwargs)
        self.assertIsNotNone(task_id_1, "Task ID должен быть возвращён.")

        task_id_2 = await self.client.submit_task(func_source, func_name, args, kwargs)
        self.assertIsNotNone(task_id_2, "Task ID должен быть возвращён.")

        async def wait_for_task_completion(task_id):
            while True:
                status = await self.client.get_task_status(task_id)
                if status["status"] == "completed":
                    return status
                await asyncio.sleep(0.1)

        status_1 = await asyncio.wait_for(wait_for_task_completion(task_id_1), timeout=10.0)
        self.assertEqual(status_1["status"], "completed", "Первая задача должна быть выполнена.")

        status_2 = await asyncio.wait_for(wait_for_task_completion(task_id_2), timeout=10.0)
        self.assertEqual(status_2["status"], "completed", "Вторая задача должна быть выполнена.")

    async def test_map(self):
        func_sq = """
        def sq(n):
            return n * n
        """
        func_name = "sq"
        args = (1, 2, 3, 4, 5)
        kwargs = {}

        task_id = await self.client.submit_task_map(func_sq, func_name, args, kwargs)

        self.assertIsNotNone(task_id, "Task ID должен быть возвращён.")

        status = await self.client.get_task_status(task_id)
        self.assertEqual(status["status"], "completed", "Задача должна быть выполнена.")
        self.assertEqual(status["result"], [1, 4, 9, 16, 25], "Результат выполнения должен быть корректным.")

    # async def test_mult_workers(self):
    #     self.assertEqual(len(self.coordinator.workers), 3, "3 воркера")





