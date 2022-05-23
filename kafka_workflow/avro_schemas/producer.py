import uuid

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
from config.config import KAFKA_TOPIC
from record.ClickRecord import ClickRecord, todict


def produce_task():
    schema_string = open("schema.avsc").read()
    schema_registry_conf = {'url': "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client,
                                     schema_str=schema_string,
                                     to_dict=todict)

    props = {"bootstrap.servers": "localhost:9092",
             "key.serializer": StringSerializer("utf_8"),
             "value.serializer": avro_serializer}

    producer = SerializingProducer(props)
    c_record = ClickRecord({"session_id": "10001", "channel": "Homepage", "ip": "localhost"})

    try:
        producer.produce(topic=KAFKA_TOPIC, key=str(uuid.uuid4()), value=c_record)
    except Exception as err:
        print(err)
    finally:
        print("successfully produced value")

    producer.flush()


if __name__ == '__main__':
    produce_task()
