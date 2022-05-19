from aiokafka import ConsumerRebalanceListener, \
    TopicPartition, OffsetAndMetadata


class RebalanceListener(ConsumerRebalanceListener):

    _current_offsets = {}

    def __init__(self, consumer):
        self.consumer = consumer

    def add_offset(self, topic, partition, offset):
        self._current_offsets[TopicPartition(topic, partition)] = OffsetAndMetadata(offset, "commit")

    def get_offsets(self):
        return self._current_offsets

    def on_partitions_assigned(self, assigned):
        print("Following partitions assigned")
        for partition in assigned:
            print(f"{partition},")

    def on_partitions_revoked(self, revoked):
        print("Following partitions revoked")
        for partition in revoked:
            print(f"{partition},")
        print("Following Partitions committed")
        for partition in self._current_offsets:
            print(partition)

        self.consumer.commit(self._current_offsets)
        self._current_offsets.clear()
