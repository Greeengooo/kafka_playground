import asyncio
import time

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from config.config import KAFKA_TOPIC
from rebalance_listener import RebalanceListener


async def consume():
    consumer = AIOKafkaConsumer(bootstrap_servers=["localhost:9092"], group_id="RG")

    # Create an instance of RebalanceListener object
    listener = RebalanceListener(consumer)
    consumer.subscribe([KAFKA_TOPIC], listener=listener)
    await consumer.start()
    try:
        async for msg in consumer:
            listener.add_offset(msg.topic, msg.partition, msg.offset)
            print(msg.value)
    except Exception as err:
        raise err
    finally:
        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(consume())
