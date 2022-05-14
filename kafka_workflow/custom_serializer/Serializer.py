from Customer import Customer
from bytebuffer import ByteBuffer


class Serializer:
    @staticmethod
    def serialize(data: Customer):
        if data is None:
            return None
        serialized_name = data.name.encode("utf-8")
        name_size = len(serialized_name)
        buff = ByteBuffer.allocate(4+4+name_size)
        buff.put_SBInt8(data.customer_id)
        buff.put_SBInt8(name_size)
        buff.put_bytes(serialized_name)
        a = buff.get_bytes()
        return buff._array
