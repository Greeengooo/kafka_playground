from kafka import KafkaProducer
from config.config import KAFKA_TOPIC


def on_send_success(metadata):
    print(f"Topic: {metadata.topic}\n"
          f"Partition: {metadata.partition},\n"
          f"Offset: {metadata.offset}")


def on_send_failure(exception):
    print(exception)


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=str.encode)
    producer.send(KAFKA_TOPIC, "hello").add_callback(on_send_success).add_errback(on_send_success)
    producer.flush()



