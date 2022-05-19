import asyncio
from config.config import KAFKA_TOPIC
from aiokafka import AIOKafkaConsumer, TopicPartition
from storage import get_offset, save_and_commit


async def consume():
    consumer = AIOKafkaConsumer(bootstrap_servers=["localhost:9092"],
                                key_deserializer=bytes.decode,
                                value_deserializer=bytes.decode,
                                enable_auto_commit=False)  # consumer will not commit the offsets at all
    p0 = TopicPartition(KAFKA_TOPIC, 0)
    p1 = TopicPartition(KAFKA_TOPIC, 1)
    p2 = TopicPartition(KAFKA_TOPIC, 2)

    consumer.assign([p0, p1, p2])
    await consumer.start()
    print(f"Current posititon "
          f"p0 = {await consumer.position(p0)}, "
          f"p1 = {await consumer.position(p1)}, "
          f"p2 = {await consumer.position(p2)}")

    consumer.seek(p0, get_offset(p0))
    consumer.seek(p1, get_offset(p1))
    consumer.seek(p2, get_offset(p2))

    print(f"New posititon "
          f"p0 = {await consumer.position(p0)}, "
          f"p1 = {await consumer.position(p1)}, "
          f"p2 = {await consumer.position(p2)}")

    try:
        async for msg in consumer:
            save_and_commit(consumer, msg)
    except Exception as err:
        raise err
    finally:
        await consumer.stop()

if __name__ == '__main__':
    asyncio.run(consume())