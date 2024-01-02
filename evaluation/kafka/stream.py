import argparse
import json
import logging
import sys

from pathlib import Path
from random import randint
from random import random
from time import time_ns
from typing import Any
from typing import Iterator
from typing import Literal
from typing import TextIO


from diaspora_event_sdk import KafkaProducer as DiasporaProducer
from diaspora_event_sdk import KafkaConsumer as DiasporaConsumer
from kafka import KafkaConsumer
from kafka import KafkaProducer

from proxystore_ex.connectors.dim.margo import MargoConnector
from proxystore_ex.connectors.dim.margo import Protocol
from proxystore.store import Store
from proxystore.store.utils import get_key

FRAMEWORK = Literal['kafka', 'diaspora']

class Producer:
    p: KafkaProducer | DiasporaProducer
    topics: dict[str, int]
    store: Store[MargoConnector] | None = None

    def __init__(
        self,
        port: int,
        bootstrap_servers: str = "0.0.0.0:9092",
        framework: FRAMEWORK = "kafka",
        proxy: bool = False
    ):
        if proxy is True:
            connector = MargoConnector(port=port, protocol=Protocol.OFI_TCP)
            self.store = Store(name="kafka_store", connector=connector)

        if "kafka" in framework:
            self.p = KafkaProducer(bootstrap_servers=bootstrap_servers)
        else:
            self.p = DiasporaProducer()

        self.topics = {}
        logging.debug("Producer initialized")

    def create_topic(self, name: str):
        self.topics[name] = 0

    @classmethod
    def serialize(cls, key: dict):
        s = json.dumps(key)
        return s.encode("utf-8")

    def publish(self, topic: str, value: Any):
        self.topics[topic] += 1
        if self.store is not None:
            proxy = self.store.proxy(value)
            event_key = {"event": self.topics[topic], "metadata": get_key(proxy)._asdict()}
            value = self.store.serializer(proxy)

        else:
            event_key = {"event": self.topics[topic]}
            value = value.encode('utf-8')

        print(topic, self.serialize(event_key), value)
        self.p.send(
            topic=topic,
            key=self.serialize(event_key),
            value=value,
        )

        if self.topics[topic] % 1 == 0:
            self.p.flush()
        logging.info(f"Event {event_key} submitted")


class Consumer:
    c: KafkaConsumer | DiasporaConsumer
    topic: str
    event_idx: int
    _iterator : Iterator
    store: Store[MargoConnector] | None = None

    def __init__(
        self,
        port: int,
        topic: str,
        bootstrap_servers: str = "0.0.0.0:9092",
        consumer_timeout_ms: int = 1000,
        framework: FRAMEWORK = 'kafka',
        proxy: bool = False
    ) -> None:
        
        if proxy is True:
            connector = MargoConnector(port=port, protocol=Protocol.OFI_TCP)
            self.store = Store(name="kafka_store", connector=connector)

        self.topic = topic

        if 'kafka' in framework:
            self.c = KafkaConsumer(
                self.topic,
                bootstrap_servers=bootstrap_servers,
                consumer_timeout_ms=consumer_timeout_ms,
                auto_offset_reset='earliest'
            )
        else:
            self.c = DiasporaConsumer(self.topic, consumer_timeout_ms=consumer_timeout_ms, auto_offset_reset='earliest')

        self.event_idx = 1

    def __iter__(self):
        return self
    
    def __next__(self):
        e = next(self.c)
        if self.store is not None:
            return (self.deserialize(e.key), self.store.deserializer(e.value))
        return (self.deserialize(e.key), e.value.decode('utf-8'))

    @classmethod
    def deserialize(cls, key: bytes):
        s = key.decode("utf-8")
        return json.loads(s)
