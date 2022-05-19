import asyncio
import time

from aiokafka import AIOKafkaProducer
from config.config import KAFKA_TOPIC


async def produce(producer: AIOKafkaProducer, value, partition):
    future = await producer.send(KAFKA_TOPIC, value, partition=partition)
    resp = await future
    print(f"key: {value} -> partition: {resp.partition}")


async def produce_task():
    producer = AIOKafkaProducer(bootstrap_servers=["localhost:9092"])
    await producer.start()
    while True:
        for _ in range(100):
            msg1 = str(time.asctime())
            await produce(producer, msg1.encode(), 0)
            time.sleep(1)
            msg2 = str(time.asctime())
            await produce(producer, msg2.encode(), 1)
            time.sleep(1)
    await producer.stop()

if __name__ == '__main__':
    asyncio.run(produce_task())
