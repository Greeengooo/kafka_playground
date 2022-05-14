from Customer import Customer


class Serializer:
    @staticmethod
    def serialize(data: Customer):
        if data is None:
            return None
        serialized_id = bytearray(data.customer_id)
        serialized_name = bytearray(data.name, "utf-8")
        return serialized_id + serialized_name
