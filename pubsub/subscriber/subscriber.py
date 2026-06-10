import sys
import os
import argparse
import pika
import time
import uuid
import json
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.coordinator_client import CoordinatorClient
from common.parser import parse_subscription_line, decrypt_string
from proto import messages_pb2

class Subscriber:
    def __init__(self, subscriber_id, file_path):
        self.subscriber_id = subscriber_id
        self.file_path = file_path
        self.coord_client = CoordinatorClient()
        retries = 15
        while retries > 0:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
                break
            except pika.exceptions.AMQPConnectionError:
                print(f"[{self.subscriber_id}] Waiting for RabbitMQ... ({retries} retries left)")
                retries -= 1
                time.sleep(2)
        else:
            raise Exception("Could not connect to RabbitMQ")
                
        self.channel = self.connection.channel()
        
        self.channel.exchange_declare(exchange='notifications_exchange', exchange_type='direct')
        result = self.channel.queue_declare(queue=f'subscriber.{self.subscriber_id}.notifications', exclusive=False)
        self.queue_name = result.method.queue
        
        self.channel.queue_bind(
            exchange='notifications_exchange',
            queue=self.queue_name,
            routing_key=self.subscriber_id
        )
        
        self.metrics = {
            'received': 0,
            'total_latency_ms': 0
        }

    def register_subscriptions(self, target_count=None, offset=0):
        with open(self.file_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
            
        if not lines:
            print("No subscriptions to register.")
            return

        brokers = self.coord_client.get_brokers()
        while not brokers:
            print("Waiting for active brokers...")
            import time
            time.sleep(1)
            brokers = self.coord_client.get_brokers()
            
        print(f"[{self.subscriber_id}] Distributing subscriptions evenly among brokers: {brokers}")
        
        count = target_count or len(lines)
        for i in range(count):
            line = lines[(offset + i) % len(lines)]
            sub = parse_subscription_line(line, subscriber_id=self.subscriber_id)
            if not sub:
                continue
                
            # Distribute balanced across brokers to satisfy the assignment requirement
            target_broker = brokers[i % len(brokers)]
            self.channel.basic_publish(
                exchange='',
                routing_key=f'broker.{target_broker}.subs',
                body=sub.SerializeToString()
            )
            
        print(f"[{self.subscriber_id}] Sent {count} subscriptions, distributed across all brokers.")

    def on_notification(self, ch, method, properties, body):
        notif = messages_pb2.Notification()
        notif.ParseFromString(body)
        
        # Decrypt string fields so the subscriber sees the original content
        for k, v in list(notif.publication.string_fields.items()):
            notif.publication.string_fields[k] = decrypt_string(v)
            
        latency = int(time.time() * 1000) - notif.publication.timestamp
        self.metrics['received'] += 1
        self.metrics['total_latency_ms'] += latency
        
        # print(f"[{self.subscriber_id}] Notification matched Sub={notif.subscription_id}, Pub={notif.publication.id}, Latency={latency}ms")
        
        # Periodically log metrics to file for evaluation
        if self.metrics['received'] % 100 == 0:
            avg_latency = self.metrics['total_latency_ms'] / self.metrics['received']
            with open(f"{self.subscriber_id}_metrics.json", "w") as f:
                json.dump(self.metrics, f)
                
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self, sub_count=None, offset=0):
        self.register_subscriptions(target_count=sub_count, offset=offset)
        print(f"[{self.subscriber_id}] Waiting for notifications...")
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.on_notification
        )
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            # Final dump
            with open(f"{self.subscriber_id}_metrics.json", "w") as f:
                json.dump(self.metrics, f)
            print(f"[{self.subscriber_id}] Stopped. Total received: {self.metrics['received']}")
            self.connection.close()
            self.coord_client.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True)
    parser.add_argument('--file', required=True)
    parser.add_argument('--count', type=int, help='Number of subscriptions to register')
    parser.add_argument('--offset', type=int, default=0, help='Start offset in subscription file')
    args = parser.parse_args()
    
    sub = Subscriber(args.id, args.file)
    sub.start(sub_count=args.count, offset=args.offset)
