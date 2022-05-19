import asyncio

import mmh3

from aiokafka import AIOKafkaProducer
from config.config import KAFKA_TOPIC

SENSOR_NAME = "TSS"


# GOAL: 30 percent of all partitions -> TSS
#       70 percent of all partitions -> SPP
def my_partitioner(key, value, all_partitions, available_partitions):
    num_partitions = len(all_partitions)
    sp = int(abs(num_partitions * 0.3))
    if key.decode() == SENSOR_NAME:
        return abs(mmh3.hash64(value)[0]) % sp
    else:
        return abs(mmh3.hash64(value)[0]) % (num_partitions - sp) + sp


async def produce(producer: AIOKafkaProducer, key, value):
    future = await producer.send(KAFKA_TOPIC, value, key=key)
    resp = await future
    print(f"key: {key} -> partition: {resp.partition}")


async def produce_task():
    producer = AIOKafkaProducer(bootstrap_servers=["localhost:9092"],
                                key_serializer=str.encode,
                                value_serializer=str.encode,
                                partitioner=my_partitioner)
    await producer.start()
    for i in range(10):
        await produce(producer, f"SPP_{i}", f"{500+i}")
    for i in range(10):
        await produce(producer, "TSS", f"TSS_SENSOR_{500+i}")
    await producer.stop()

if __name__ == '__main__':
    asyncio.run(produce_task())
