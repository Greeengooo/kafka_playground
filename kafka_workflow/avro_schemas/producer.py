import asyncio
from aiokafka import AIOKafkaProducer
from config.config import KAFKA_TOPIC
from record import ClickRecord


async def produce(producer: AIOKafkaProducer, value):
    future = await producer.send(KAFKA_TOPIC, value)
    resp = await future
    print(f"key: {value} -> partition: {resp.partition}")


async def produce_task():
    producer = AIOKafkaProducer(bootstrap_servers=["localhost:9092"],
                                value_serializer=ClickRecord.serialize)
    c_record = ClickRecord({"session_id": "10001", "channel": "Homepage", "ip": "localhost"})

    await producer.start()
    await produce(producer, c_record)
    await producer.stop()

if __name__ == '__main__':
    asyncio.run(produce_task())
