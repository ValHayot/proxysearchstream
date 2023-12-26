import argparse
import logging
import subprocess
import string
import sys

from random import randint
from random import choices
from time import sleep
from time import time_ns
from typing import TextIO

from globus_compute_sdk import Executor

from evaluation.kafka.stream import Consumer
from evaluation.kafka.stream import Producer

def run_kafka_consumer(topic: str, bootstrap_servers: str, port:str) -> str:
    import logging
    import sys

    from time import sleep
    from time import time_ns

    from evaluation.kafka.stream import Consumer

    sleep(1)
    c = Consumer(topic = topic, bootstrap_servers=bootstrap_servers, port=port)
    logging.debug('consumer started')

    output: str = ""
    for e,v in c:
        start = time_ns()
        logging.debug(e)
        end = time_ns()
        duration = end - start

        print(f'Received event {e} in {end - start} nanoseconds with data {v}')
        output += f'kafka,{e["event"]},consume,{len(v)},{duration}\n' 
    return output

def run_mofka_consumer(num_events: int, benchmark_file: str):
    import subprocess 

    s = subprocess.run(
        [
            '/Users/valeriehayot-sasson/postdoc/proxystream/evaluation/mofka/build/consumer',
            '-p',
            'ofi+tcp',
            '-s',
            'mofka.ssg',
            '-e',
            str(num_events),
            '-f',
            benchmark_file
        ],
        check=True,
        capture_output=True
    )
    print(s.stdout, s.stderr)
    return s.stdout

    

def launch_kafka(bf:str, endpoint:str, broker: str, port:int, num_events:int, size: int):
    f: TextIO | None = None

    if bf is not None:
        f = open(bf, 'a+')
    
    # start consumer before producer to get events as producer is producing them 
    with Executor(endpoint_id=endpoint) as gce:    
        future = gce.submit(run_kafka_consumer, topic, broker, port)

        p = Producer(bootstrap_servers=broker, port=port)
        p.create_topic(topic)

        for i in range(num_events):
            start = time_ns()
            data = ''.join(choices(string.ascii_uppercase, k=size))
            p.publish(topic, data)
            end = time_ns()
            duration = end - start

            print(f'Published data {data} in {duration} nanoseconds')

            if f is not None:
                f.write(f'kafka,{i},publish,{len(data)},{duration}\n')

        if f is not None:
            f.write(future.result())

    if f is not None:
        f.close()

if __name__=='__main__':

    parser = argparse.ArgumentParser(
                    prog='Kafka (proxy) Stream',
                    description='Transmits proxied data as a stream using Kafka',
                )

    parser.add_argument('type', choices=['kafka', 'diaspora', 'mofka'], help='Stream framework type')
    
    parser.add_argument('-e', '--endpoint', type=str, help='Globus Compute Endpoint UUID')
    parser.add_argument('-s', '--size', type=int, default=8, help='Data size')
    parser.add_argument('-n', '--num-events', type=int, default=10, help='Number of events to transmit')
    parser.add_argument('-p', '--port', type=int, default=randint(40000, 70000), help='Port to use for MargoConnector')
    parser.add_argument('-b', '--broker', type=str, default='0.0.0.0:9092', help='Kafka broker address')
    parser.add_argument('-l', '--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='ERROR')
    parser.add_argument('-f', '--benchmark-file', type=str, default=None, help='File to save benchmark data to (will print to console otherwise)')

    args = parser.parse_args()
    
    level = logging.getLevelName(args.log_level)    
    logging.basicConfig(level=level, handlers=[logging.StreamHandler()])

    topic = 'stream-1'

    if args.type == 'kafka': 
        launch_kafka(
            bf=args.benchmark_file,
            endpoint=args.endpoint,
            broker=args.broker,
            port=args.port,
            num_events=args.num_events,
            size=args.size
        )
    elif args.type == 'mofka':
        # run producer
        s = subprocess.run(
            [
                './evaluation/mofka/build/producer',
                '-p',
                'ofi+tcp',
                '-s',
                'mofka.ssg',
                '-e',
                str(args.num_events),
                '-f',
                args.benchmark_file
            ],
            check=True,
            capture_output=True
        )
        
        print(s.stdout)
        
        with Executor(endpoint_id=args.endpoint) as gce:    
            future = gce.submit(run_mofka_consumer, args.num_events, args.benchmark_file)
            _ = future.result()