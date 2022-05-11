from kafka import KafkaConsumer
from config.config import KAFKA_TOPIC

if __name__ == '__main__':
    consumer = KafkaConsumer(KAFKA_TOPIC)
    for message in consumer:
        print(message)