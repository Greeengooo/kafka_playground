import asyncio
import io

import avro.schema
from aiokafka import AIOKafkaConsumer
from config.config import KAFKA_TOPIC
from avro.io import DatumReader, BinaryDecoder


def deserialize(msg_value):
    schema = avro.schema.parse(open("schema.avsc").read())
    reader = DatumReader(schema)
    msg_bytes = io.BytesIO(msg_value)
    decoder = BinaryDecoder(msg_bytes)
    event_dict = reader.read(decoder)
    return event_dict


async def consume():
    consumer = AIOKafkaConsumer(bootstrap_servers=["localhost:9092"],
                                value_deserializer=deserialize)
    consumer.subscribe([KAFKA_TOPIC])
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"value={msg.value}")
            await consumer.commit()
    except Exception as err:
        raise err
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(consume())