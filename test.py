import unittest
import asyncio
from client import Client
from coord import Coordinator
from worker import Worker


class TestSystemExtended(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Настройка координатора, воркера и клиента
        self.coord_host = "localhost"
        self.coord_port = 8765

        self.coordinator = Coordinator(self.coord_host, self.coord_port)
        self.worker = Worker(self.coord_host, self.coord_port)
        self.client = Client(self.coord_host, self.coord_port)

        self.coord_server = asyncio.create_task(self.coordinator.start_server())
        await asyncio.sleep(0.1)  # Ожидание запуска сервера

        self.worker_task = asyncio.create_task(self.worker.connect_to_coord())
        await asyncio.sleep(0.1)  # Даем время на подключение

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
