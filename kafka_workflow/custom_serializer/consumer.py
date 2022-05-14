import asyncio

from aiokafka import AIOKafkaConsumer
from config.config import KAFKA_TOPIC
from Deserializer import Deserializer


async def consume():
    consumer = AIOKafkaConsumer(KAFKA_TOPIC,
                                bootstrap_servers=["localhost:9092"],
                                value_deserializer=Deserializer().deserialize)
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"value={msg.value}")
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(consume())