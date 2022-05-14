from kafka import KafkaProducer
from config.config import KAFKA_TOPIC

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             key_serializer=str.encode,
                             value_serializer=str.encode)
    producer.send(KAFKA_TOPIC, key="hello", value="world")
    producer.flush()


