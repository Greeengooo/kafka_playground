from Customer import Customer


class Deserializer:
    @staticmethod
    def deserialize(data: bytes):
        if data is None:
            return None
        customer_id = int(data[0])
        name = str(data[1])
        return Customer(customer_id, name)
