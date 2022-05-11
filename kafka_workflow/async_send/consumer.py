from kafka import KafkaConsumer
if __name__ == '__main__':
    consumer = KafkaConsumer('kafka_tutorial')
    for message in consumer:
        print(message)