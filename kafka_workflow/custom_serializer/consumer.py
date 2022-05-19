import asyncio

from aiokafka import AIOKafkaConsumer
from config.config import KAFKA_TOPIC
from Deserializer import Deserializer


async def consume():
    consumer = AIOKafkaConsumer(bootstrap_servers=["localhost:9092"],
                                value_deserializer=Deserializer().deserialize)
    consumer.subscribe([KAFKA_TOPIC])
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"value={msg.value}")
            await consumer.commit()
    except Exception as err:
        print(err)
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(consume())