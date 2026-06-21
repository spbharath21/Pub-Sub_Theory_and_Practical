class Partition:
    # Create a partition with its own append-only log.
    def __init__(self, partition_id):
        self.partition_id = partition_id
        self.messages = []

    # Append a message and return its offset.
    def append(self, message):
        offset = len(self.messages)
        self.messages.append(message)
        return offset

    # Return all messages starting from an offset.
    def read_from(self, offset):
        return self.messages[offset:]


class Topic:
    # Create a topic containing multiple partitions.
    def __init__(self, name, num_partitions):
        self.name = name
        self.partitions = [
            Partition(i) for i in range(num_partitions)
        ]

    # Choose a partition based on the message key.
    def choose_partition(self, key):
        return hash(key) % len(self.partitions)

    # Store a message inside the chosen partition.
    def publish(self, key, message):
        partition_id = self.choose_partition(key)
        partition = self.partitions[partition_id]

        offset = partition.append(message)

        return partition_id, offset


class Broker:
    # Create a broker that owns all topics.
    def __init__(self):
        self.topics = {}

    # Create a topic if it does not already exist.
    def create_topic(self, topic_name, partitions=3):
        if topic_name not in self.topics:
            self.topics[topic_name] = Topic(
                topic_name,
                partitions,
            )

    # Publish a message into a topic.
    def publish(self, topic_name, key, message):
        self.create_topic(topic_name)
        return self.topics[topic_name].publish(key, message)

    # Return a topic object.
    def get_topic(self, topic_name):
        return self.topics[topic_name]


class Producer:
    # Initialize the producer with a broker.
    def __init__(self, broker):
        self.broker = broker

    # Publish a keyed message.
    def publish(self, topic_name, key, message):
        partition, offset = self.broker.publish(
            topic_name,
            key,
            message,
        )

        print(
            f"Produced -> "
            f"partition={partition}, "
            f"offset={offset}"
        )


class Consumer:
    # Initialize offsets for every partition.
    def __init__(self, broker, topic_name):
        self.topic = broker.get_topic(topic_name)

        self.offsets = {
            partition.partition_id: 0
            for partition in self.topic.partitions
        }

    # Poll all partitions for unread messages.
    def poll(self):
        for partition in self.topic.partitions:

            current_offset = self.offsets[
                partition.partition_id
            ]

            messages = partition.read_from(current_offset)

            for message in messages:
                print(
                    f"Consumed "
                    f"(partition={partition.partition_id}, "
                    f"offset={current_offset}) "
                    f"-> {message}"
                )

                current_offset += 1

            self.offsets[
                partition.partition_id
            ] = current_offset


if __name__ == "__main__":
    broker = Broker()

    broker.create_topic(
        "rides",
        partitions=3,
    )

    producer = Producer(broker)

    producer.publish(
        "rides",
        "driver_1",
        "Ping A",
    )

    producer.publish(
        "rides",
        "driver_2",
        "Ping B",
    )

    producer.publish(
        "rides",
        "driver_1",
        "Ping C",
    )

    consumer = Consumer(
        broker,
        "rides",
    )

    print("\nFirst poll:")
    consumer.poll()

    producer.publish(
        "rides",
        "driver_2",
        "Ping D",
    )

    producer.publish(
        "rides",
        "driver_1",
        "Ping E",
    )

    print("\nSecond poll:")
    consumer.poll()