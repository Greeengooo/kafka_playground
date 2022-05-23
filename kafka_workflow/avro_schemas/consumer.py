from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from record.ClickRecord import todict
from config.config import KAFKA_TOPIC


def consume():
    schema_string = open("schema.avsc").read()
    sr_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_string,
                                         todict)

    consumer_conf = {'bootstrap.servers': "localhost:9092",
                     'key.deserializer': StringDeserializer('utf_8'),
                     'value.deserializer': avro_deserializer,
                     'group.id': 'test'}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            record = msg.value()
            if record is not None:
                print(record)
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':
    consume()