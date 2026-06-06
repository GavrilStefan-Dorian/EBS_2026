import sys
import os
import argparse
import pika
import time
import random
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.coordinator_client import CoordinatorClient
from common.parser import parse_publication_line

class Publisher:
    def __init__(self, publisher_id, file_path):
        self.publisher_id = publisher_id
        self.file_path = file_path
        self.coord_client = CoordinatorClient()
        retries = 15
        while retries > 0:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
                break
            except pika.exceptions.AMQPConnectionError:
                print(f"[{self.publisher_id}] Waiting for RabbitMQ... ({retries} retries left)")
                retries -= 1
                time.sleep(2)
        else:
            raise Exception("Could not connect to RabbitMQ")
                
        self.channel = self.connection.channel()
        
    def start(self, duration_sec=None, rate_per_sec=None):
        with open(self.file_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
            
        if not lines:
            print("No publications to send.")
            return

        print(f"[{self.publisher_id}] Starting to publish...")
        start_time = time.time()
        sent_count = 0
        
        while True:
            if duration_sec and (time.time() - start_time > duration_sec):
                break
                
            line = random.choice(lines)
            pub = parse_publication_line(line, publisher_id=self.publisher_id)
            if not pub:
                continue
                
            brokers = self.coord_client.get_brokers()
            if not brokers:
                print("No brokers available, waiting...")
                time.sleep(1)
                continue
                
            broker = random.choice(brokers)
            
            pub.timestamp = int(time.time() * 1000)
            
            self.channel.basic_publish(
                exchange='',
                routing_key=f'broker.{broker}.pubs',
                body=pub.SerializeToString()
            )
            sent_count += 1
            
            if rate_per_sec:
                time.sleep(1.0 / rate_per_sec)
            else:
                time.sleep(0.01) # small delay to not overwhelm
                
            if not duration_sec and sent_count >= len(lines):
                break
                
        print(f"[{self.publisher_id}] Finished publishing. Sent {sent_count} messages.")
        with open(f"{self.publisher_id}_metrics.json", "w") as f:
            json.dump({"sent": sent_count}, f)
        self.connection.close()
        self.coord_client.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True)
    parser.add_argument('--file', required=True)
    parser.add_argument('--duration', type=int, help='Duration in seconds to publish')
    parser.add_argument('--rate', type=int, help='Messages per second')
    args = parser.parse_args()
    
    pub = Publisher(args.id, args.file)
    pub.start(duration_sec=args.duration, rate_per_sec=args.rate)
