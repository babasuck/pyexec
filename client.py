import pickle
import asyncio
import inspect
from typing import Any


async def send_function(func: Any, *args: Any, **kwargs: Any) -> None:
    func_source = inspect.getsource(func)
    func_data = pickle.dumps((func_source, args, kwargs))
    reader, writer = await asyncio.open_connection('localhost', 12345)
    writer.write(func_data)
    await writer.drain()

    print(f"Задача отправлена: {func.__name__}")
    while not reader.at_eof():
        data = await reader.read(4096)
        if data:
            status_info = pickle.loads(data)
            task_id = status_info["task_id"]
            status = status_info["status"]
            result = status_info.get("result")
            print(f"Задача {task_id}: {status}")
            if status == "completed":
                print(f"Результат: {result}")
                break

    writer.close()
    await writer.wait_closed()


def sum(a: int, b: int) -> int:
    result = 0
    for _ in range(10**7):
        result += a * b
        result -= a
        result += b
    return a + b


def multiply(a: int, b: int) -> int:
    result = 1
    for _ in range(10**6):
        result *= (a + b) % 10
    return a * b


async def main() -> None:
    await asyncio.gather(
        send_function(sum, 10, 20),
        send_function(multiply, 5, 6)
    )


if __name__ == '__main__':
    asyncio.run(main())
