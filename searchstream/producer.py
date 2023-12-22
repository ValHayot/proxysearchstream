import logging

from random import random
from globus_sdk import SearchClient

from proxystore.connectors.file import FileConnector
from proxystore.store import Store
from proxystore.store.utils import get_key

from globus_auth import create_authorizer
from utils import get_ip
from utils import JSON
from utils import STORE_DIR


class Producer:
    index_uuid: str
    topics: dict[str, int]  # value is the last event index
    store: Store[FileConnector]
    client: SearchClient

    def __init__(self, index_uuid: str | None = None):
        # configure proxystore
        connector = FileConnector(STORE_DIR)
        ip = get_ip()
        self.store = Store(name=f"{ip}-streams", connector=connector)

        # create index
        authorizer = create_authorizer()
        self.client = SearchClient(authorizer=authorizer)

        if index_uuid is None:
            self.index_uuid = self.create_producer_index(
                ip, f"Search index for ip {ip}"
            )
            print(self.index_uuid)
        else:
            self.index_uuid = index_uuid

        self.topics = {}

    def create_topic(self, name: str):
        """Initializes a topic

        Args:
            name (str): Topic name
        """
        self.topics[name] = 0

    def create_search_entry(self, topic: str, event: JSON) -> None:
        """Create entry for particular topic in search index

        Args:
            topic (str): Topic name
            event (JSON): Event to store in index
        """
        ingest_data = {
            "ingest_type": "GMetaEntry",
            "ingest_data": {
                "subject": topic,
                "id": str(self.topics[topic]),
                "visible_to": ["public"],
                "content": event,
            },
        }
        self.client.ingest(self.index_uuid, ingest_data)

    def create_producer_index(self, index_name: str, index_description: str) -> str:
        """Create the search index for this producer

        Args:
            index_name (str): Name of the index
            index_description (str): Description to associate with the index

        Returns:
            str: Index UUID
        """
        r = self.client.create_index(index_name, index_description)
        return r["id"]

    def publish(self, data: JSON, topic_name: str, end: bool = False) -> JSON:
        """Proxy data and publish event to topic

        Args:
            data (JSON): The data to proxy whose metadata will be returned by event
            topic_name (str): Topic associated with the stream
            end (bool, optional): If this is the last event in the stream. Defaults to False.

        Returns:
            JSON: the stream event
        """
        p = self.store.proxy(data)
        metadata = get_key(p)._asdict()
        self.topics[topic_name] += 1
        index = self.topics[topic_name]
        event = {"event": index, "metadata": metadata, "end": end}
        logging.info(f"Publishing {event=} to {self.index_uuid=} and {topic_name=}")

        self.create_search_entry(topic=topic_name, event=event)
        return event


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
    index = "fb3cfb2b-99dc-43d0-a9d2-1cbbde66ac4a"
    topic = "stream-1"

    p = Producer(index_uuid=index)
    p.create_topic(topic)
    for i in range(8):
        if i < 7:
            e = p.publish(random(), topic)
        else:
            e = p.publish(random(), topic, True)
