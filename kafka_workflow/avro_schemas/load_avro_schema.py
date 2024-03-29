from confluent_kafka import avro


def load_avro_schema_from_file():
    with open("schema.avsc") as schema_file:
        key_schema_string = """
        {"type": "string"}
        """
        key_schema = avro.loads(key_schema_string)
        value_schema = avro.load(schema_file.name)
    return key_schema, value_schema
