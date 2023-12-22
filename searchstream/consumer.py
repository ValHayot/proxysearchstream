import logging

from typing import Any

from globus_sdk import SearchClient
from globus_sdk.services.search.errors import SearchAPIError

from proxystore.connectors.file import FileConnector
from proxystore.connectors.file import FileKey
from proxystore.store import Store

from globus_auth import create_authorizer
from utils import get_ip
from utils import JSON
from utils import STORE_DIR


class Consumer:
    index_uuid: str
    topic: str
    event_idx: int
    store: Store[FileConnector]
    client: SearchClient

    # equivalent to subscribe for now
    def __init__(self, index_uuid: str, topic: str):
        self.index_uuid = index_uuid
        self.topic = topic

        ip = get_ip()

        # Create proxystore connector
        connector = FileConnector(STORE_DIR)
        self.store = Store(name=f"{ip}-stream-consumer", connector=connector)

        # Configure SearchClient
        authorizer = create_authorizer()
        self.client = SearchClient(authorizer=authorizer)

        self.event_idx = 1

    def get(self) -> JSON:
        """Get next event in the stream

        Raises:
            Exception: When topic is closed

        Returns:
            JSON: the event contents
        """
        while (
            self.client.get_index(self.index_uuid)["status"] == "open"
            and self.event_idx > 0
        ):
            try:
                entry = self.client.get_entry(
                    self.index_uuid, self.topic, entry_id=str(self.event_idx)
                )
                event = entry["entries"][0]["content"]

                logging.info(f"Received event {event}")

                if event["end"] == False:
                    self.event_idx += 1
                else:
                    self.event_idx = -1
                return event
            except SearchAPIError as e:
                logging.debug(e)
        raise Exception("Topic is closed")

    def get_event_data(self, event: dict) -> Any:
        """Resolve the Proxy associated with the key

        Args:
            event (dict): the stream event

        Returns:
            _type_: _description_
        """
        return self.store.get(FileKey(**event["metadata"]))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])

    index = "fb3cfb2b-99dc-43d0-a9d2-1cbbde66ac4a"
    topic = "stream-1"

    c = Consumer(index_uuid=index, topic=topic)
    for i in range(9):
        event = c.get()
        print(c.get_event_data(event))
