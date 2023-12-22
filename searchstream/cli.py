import logging
import sys

from consumer import Consumer
from producer import Producer

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])

    index = "fb3cfb2b-99dc-43d0-a9d2-1cbbde66ac4a"
    p = Producer(index_uuid=index)
    topic = "stream-1"
    p.create_topic(topic)
    e = p.publish("Hello World", topic)

    c = Consumer(index_uuid=index, topic=topic)
    event = c.get()
    print(c.get_event_data(event))
