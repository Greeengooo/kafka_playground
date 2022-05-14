import asyncio
from aiokafka import AIOKafkaProducer
from config.config import KAFKA_TOPIC
from Customer import Customer
from Serializer import Serializer


async def produce(producer: AIOKafkaProducer, value):
    future = await producer.send(KAFKA_TOPIC, value)
    resp = await future
    print(f"key: {value} -> partition: {resp.partition}")


async def produce_task():
    producer = AIOKafkaProducer(bootstrap_servers=["localhost:9092"],
                                value_serializer=Serializer().serialize)
    await producer.start()
    await produce(producer, Customer(customer_id=1, name="Hello"))
    await producer.stop()

if __name__ == '__main__':
    asyncio.run(produce_task())
