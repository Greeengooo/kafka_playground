from kafka import KafkaProducer

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             key_serializer=str.encode,
                             value_serializer=str.encode)
    producer.send('kafka_tutorial', key="hello", value="world")
    producer.flush()


