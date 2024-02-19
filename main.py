import asyncio
from enum import Enum
from typing import List
from datetime import timedelta
from dataclasses import dataclass
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import random
import tracemalloc


timeout_seconds = timedelta(seconds=15).total_seconds()

CLIENTS = ["Tinkoff Investments", "Tinkoff Bank", "Oleg"]
CONTENTS = ["Help!", "Nice!", "What is going on?"]

@dataclass
class Payload:
    content: str


@dataclass
class Address:
    client: str


@dataclass
class Event:
    recipients: List[Address]
    payload: Payload


class Result(Enum):
    Accepted = 1
    Rejected = 2


async def read_data() -> Event:
    # Метод для чтения данных
    event = await data_randomizer()
    print(f"Data {event.payload} was received for sending to recipients {event.recipients}")
    return event


async def data_randomizer() -> Event:
    clients_value: int = random.randrange(1, 4)
    actual_clients = CLIENTS[:clients_value]
    await asyncio.sleep(0.2)
    recipients = [Address(client) for client in actual_clients]
    content = random.choice(CONTENTS)
    payload = Payload(content)
    return Event(recipients, payload)


async def send_data(dest: Address, payload: Payload) -> Result:
    # Метод для рассылки данных
    result = await response_mock()
    while result.value != 1:
        print(f"Data {payload} was not sent to address {dest}. Trying another time in 0.2 sec...")
        await asyncio.sleep(0.2)
        result = await response_mock()
    print(f"Data {payload} was sent to address {dest}. Response: {result.name}, code: {result.value}")
    return result


async def response_mock() -> Result:
    result_num = random.randrange(1, 11)
    await asyncio.sleep(0.2)
    if result_num in range(9):
        return Result(1)
    else:
        return Result(2)


async def perform_operation() -> None:
    tracemalloc.start()
    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            future_1 = executor.submit(read_data)
            result_1 = await future_1.result(timeout=timeout_seconds)
            future_to_send = {executor.submit(send_data, recipient, result_1.payload): recipient for recipient in result_1.recipients}
            for future in concurrent.futures.as_completed(future_to_send):
                await future.result()


# Запуск асинхронного кода
if __name__ == "__main__":
    asyncio.run(perform_operation())
