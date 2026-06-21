class Subscriber:
    # Initialize a subscriber with a name.
    def __init__(self, name):
        self.name = name

    # Receive a message pushed by the broker.
    def receive(self, topic, message):
        print(f"[{self.name}] received from {topic}: {message}")


class Topic:
    # Create a topic with subscribers and message history.
    def __init__(self, name):
        self.name = name
        self.subscribers = []
        self.messages = []

    # Register a subscriber to this topic.
    def subscribe(self, subscriber):
        self.subscribers.append(subscriber)

    # Publish a message and immediately notify subscribers.
    def publish(self, message):
        self.messages.append(message)

        for subscriber in self.subscribers:
            subscriber.receive(self.name, message)


class Broker:
    # Create a broker that manages topics.
    def __init__(self):
        self.topics = {}

    # Return an existing topic or create it if absent.
    def get_topic(self, topic_name):
        if topic_name not in self.topics:
            self.topics[topic_name] = Topic(topic_name)

        return self.topics[topic_name]

    # Subscribe a subscriber to a topic.
    def subscribe(self, topic_name, subscriber):
        topic = self.get_topic(topic_name)
        topic.subscribe(subscriber)

    # Route a published message to the correct topic.
    def publish(self, topic_name, message):
        topic = self.get_topic(topic_name)
        topic.publish(message)


class Publisher:
    # Initialize the publisher with a broker reference.
    def __init__(self, broker):
        self.broker = broker

    # Send a message to the broker.
    def publish(self, topic_name, message):
        self.broker.publish(topic_name, message)


if __name__ == "__main__":
    broker = Broker()

    alice = Subscriber("Alice")
    bob = Subscriber("Bob")

    broker.subscribe("sports", alice)
    broker.subscribe("sports", bob)

    publisher = Publisher(broker)

    publisher.publish("sports", "India won!")
    publisher.publish("sports", "Another update")