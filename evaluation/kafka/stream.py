import argparse
import json
import logging
import sys

from pathlib import Path
from random import randint
from random import random
from time import time_ns
from typing import Any
from typing import TextIO
from kafka import KafkaConsumer
from kafka import KafkaProducer

from proxystore_ex.connectors.dim.margo import MargoConnector
from proxystore_ex.connectors.dim.margo import Protocol 
from proxystore.store import Store
from proxystore.store.utils import get_key

class Producer:
    p: KafkaProducer
    store: Store[MargoConnector]
    topics: dict[str, int]
    
    def __init__(self, port: int, bootstrap_servers: str = '0.0.0.0:9092'):
        connector = MargoConnector(port=port, protocol=Protocol.OFI_TCP)
        self.store = Store(name='kafka_store', connector=connector)
        self.p = KafkaProducer(bootstrap_servers = bootstrap_servers)
        self.topics = {}
        logging.debug('Producer initialized')
        
    def create_topic(self, name: str):
        self.topics[name] = 0

    @classmethod
    def serialize(cls, key: dict):
        s = json.dumps(key)
        return s.encode('utf-8')
        
    def publish(self, topic:str, value: Any):
        proxy = self.store.proxy(value)

        self.topics[topic] += 1
        event_key = { 'event': self.topics[topic], 'metadata': get_key(proxy)._asdict()}

        self.p.send(
            topic=topic,
            key=self.serialize(event_key),
            value=self.store.serializer(proxy)
        )

        if self.topics[topic] % 100 == 0:
            self.p.flush()
        logging.info(f'Event {event_key} submitted')

class Consumer:
    c: KafkaConsumer
    topic: str
    event_idx: int
    store: Store[MargoConnector]

    def __init__(self, port:int, topic:str, bootstrap_servers: str = '0.0.0.0:9092', consumer_timeout_ms: int = 1000) -> None:
        connector = MargoConnector(port=port, protocol=Protocol.OFI_TCP)
        self.store = Store(name='kafka_store', connector=connector)
        self.topic = topic
        self.c = KafkaConsumer(self.topic, bootstrap_servers=bootstrap_servers, consumer_timeout_ms=consumer_timeout_ms)
        self.event_idx = 1

    def __iter__(self):
        return self
    
    def __next__(self):
        if (event := next(self.c)) is not None:
            return self.deserialize(event.key), self.store.deserializer(event.value) # f'{self.deserialize(event.key)} with data {self.store.deserializer(event.value)}'
    
    @classmethod
    def deserialize(cls, key: bytes):
        s = key.decode('utf-8')
        return json.loads(s)
