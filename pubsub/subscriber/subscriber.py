import sys
import os
import argparse
import pika
import time
import uuid
import json
import random
import signal

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.coordinator_client import CoordinatorClient
from common.parser import parse_subscription_line, decrypt_string
from proto import messages_pb2

class Subscriber:
    def __init__(self, subscriber_id, file_path, replication_factor=1):
        self.subscriber_id = subscriber_id
        self.file_path = file_path
        self.replication_factor = replication_factor
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
        self.channel.basic_qos(prefetch_count=100)
        self.channel.exchange_declare(exchange='notifications_exchange', exchange_type='direct')
        result = self.channel.queue_declare(queue=f'subscriber.{self.subscriber_id}.notifications', exclusive=False, durable=True)
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
        
        self.seen_notifications = set()

    def register_subscriptions(self, target_count=None, offset=0):
        with open(self.file_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
            
        if not lines:
            print("No subscriptions to register.")
            return

        brokers = self.coord_client.get_brokers()
        brokers = sorted(brokers)
        
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
                
            primary_index = i % len(brokers)

            target_brokers = []
            for r in range(min(self.replication_factor, len(brokers))):
                target_brokers.append(brokers[(primary_index + r) % len(brokers)])

            for target_broker in set(target_brokers):
                self.channel.basic_publish(
                    exchange='',
                    routing_key=f'broker.{target_broker}.subs',
                    body=sub.SerializeToString(),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            
        print(f"[{self.subscriber_id}] Sent {count} subscriptions, distributed across all brokers.")

    def on_notification(self, ch, method, properties, body):
        notif = messages_pb2.Notification()
        notif.ParseFromString(body)
        key = (notif.subscription_id, notif.publication.id)

        if key in self.seen_notifications:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        self.seen_notifications.add(key)
        
        # Decrypt string fields so the subscriber sees the original content
        for k, v in list(notif.publication.string_fields.items()):
            notif.publication.string_fields[k] = decrypt_string(v)
            
        latency = int(time.time() * 1000) - notif.publication.timestamp
        self.metrics['received'] += 1
        self.metrics['total_latency_ms'] += latency
        
        # print(f"[{self.subscriber_id}] Notification matched Sub={notif.subscription_id}, Pub={notif.publication.id}, Latency={latency}ms")
        
        # Periodically log metrics to file for evaluation
        if self.metrics['received'] % 10000 == 0:
            avg_latency = self.metrics['total_latency_ms'] / self.metrics['received']
            with open(f"{self.subscriber_id}_metrics.json", "w") as f:
                json.dump(self.metrics, f)
                
        ch.basic_ack(delivery_tag=method.delivery_tag)


    def save_metrics_and_close(self):
        with open(f"{self.subscriber_id}_metrics.json", "w") as f:
            json.dump(self.metrics, f)

        print(f"[{self.subscriber_id}] Stopped. Total received: {self.metrics['received']}")

        try:
            if self.channel.is_open:
                self.channel.stop_consuming()
        except Exception:
            pass

        try:
            if self.connection.is_open:
                self.connection.close()
        except Exception:
            pass

        try:
            self.coord_client.close()
        except Exception:
            pass

    def handle_stop_signal(self, signum, frame):
        self.save_metrics_and_close()
        sys.exit(0)
        
        
    def start(self, sub_count=None, offset=0):
        signal.signal(signal.SIGINT, self.handle_stop_signal)
        signal.signal(signal.SIGTERM, self.handle_stop_signal)

        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, self.handle_stop_signal)
            
        self.register_subscriptions(target_count=sub_count, offset=offset)
        print(f"[{self.subscriber_id}] Waiting for notifications...")
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.on_notification
        )
        
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.save_metrics_and_close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True)
    parser.add_argument('--file', required=True)
    parser.add_argument('--count', type=int, help='Number of subscriptions to register')
    parser.add_argument('--offset', type=int, default=0, help='Start offset in subscription file')
    parser.add_argument('--replication-factor', type=int, default=1)
    args = parser.parse_args()
    
    sub = Subscriber(args.id, args.file, replication_factor=args.replication_factor)
    sub.start(sub_count=args.count, offset=args.offset)
