from kafka import KafkaProducer
from kafka.errors import KafkaError
from config.config import KAFKA_TOPIC

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             key_serializer=str.encode,
                             value_serializer=str.encode)
    try:
        response = producer.send(KAFKA_TOPIC, key="hello", value="world")
        metadata = response.get()
        print(f"Topic: {metadata.topic}\n"
              f"Partition: {metadata.partition},\n"
              f"Offset: {metadata.offset}")
    except KafkaError as err:
        print(err)
    producer.flush()


