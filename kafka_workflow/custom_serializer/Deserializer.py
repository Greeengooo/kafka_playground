from Customer import Customer
from bytebuffer import ByteBuffer


class Deserializer:
    @staticmethod
    def deserialize(data: bytes):
        if data is None:
            return None
        buff = ByteBuffer.wrap(bytearray(data))
        customer_id = buff.get_SBInt8()
        size_of_name = buff.get_SBInt8()
        name_bytes = bytearray(size_of_name)
        buff.get(name_bytes)
        deserialized_name = name_bytes.decode("utf-8")
        return Customer(customer_id, deserialized_name)
