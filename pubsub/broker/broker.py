import sys
import threading
import argparse
import pika
import time
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.coordinator_client import send_heartbeat, CoordinatorClient
from common.matcher import matches
from proto import messages_pb2

class Broker:
    def __init__(self, broker_id):
        self.broker_id = broker_id
        
        self.local_subscriptions = [] # Subscriptions stored on this broker
        self.routing_table = [] # Tuples of (messages_pb2.Subscription, target_broker_id)
        
        self.coord_client = CoordinatorClient()
        
        retries = 15
        while retries > 0:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
                break
            except pika.exceptions.AMQPConnectionError:
                print(f"[{self.broker_id}] Waiting for RabbitMQ... ({retries} retries left)")
                retries -= 1
                time.sleep(2)
        else:
            raise Exception("Could not connect to RabbitMQ")
                
        self.channel = self.connection.channel()
        
        # 1. Subscriptions from subscribers
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.subs')
        
        # 2. Routing entries from other brokers (flooded subscriptions)
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.routing')
        
        # 3. Publications from publishers (entry point)
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.pubs')
        
        # 4. Publications forwarded from other brokers
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.forwarded_pubs')
        
        # Notifications exchange
        self.channel.exchange_declare(exchange='notifications_exchange', exchange_type='direct')

    def start_heartbeat(self):
        threading.Thread(target=send_heartbeat, args=(self.broker_id,), daemon=True).start()

    def handle_subscription(self, ch, method, properties, body):
        sub = messages_pb2.Subscription()
        sub.ParseFromString(body)
        
        # Store it locally
        self.local_subscriptions.append(sub)
        print(f"[{self.broker_id}] Stored local subscription {sub.id}")
        
        # Flood it to all other brokers as a Routing Entry
        brokers = self.coord_client.get_brokers()
        
        routing_entry = messages_pb2.RoutingEntry()
        routing_entry.source_broker = self.broker_id
        routing_entry.subscription.CopyFrom(sub)
        
        for other_broker in brokers:
            if other_broker != self.broker_id:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=f'broker.{other_broker}.routing',
                    body=routing_entry.SerializeToString()
                )
                
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_routing_entry(self, ch, method, properties, body):
        entry = messages_pb2.RoutingEntry()
        entry.ParseFromString(body)
        
        self.routing_table.append((entry.subscription, entry.source_broker))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _match_local_and_notify(self, pub):
        for sub in self.local_subscriptions:
            if matches(pub, sub):
                notif = messages_pb2.Notification()
                notif.subscription_id = sub.id
                notif.publication.CopyFrom(pub)
                
                self.channel.basic_publish(
                    exchange='notifications_exchange',
                    routing_key=sub.subscriber_id,
                    body=notif.SerializeToString()
                )

    def handle_publication(self, ch, method, properties, body):
        pub = messages_pb2.Publication()
        pub.ParseFromString(body)
        
        # 1. Match against local subscriptions
        self._match_local_and_notify(pub)
                
        # 2. Match against routing table to find which brokers need this publication
        target_brokers = set()
        for routing_sub, target_broker in self.routing_table:
            if matches(pub, routing_sub):
                target_brokers.add(target_broker)
                
        # 3. Forward to the responsible brokers
        for target_broker in target_brokers:
            self.channel.basic_publish(
                exchange='',
                routing_key=f'broker.{target_broker}.forwarded_pubs',
                body=pub.SerializeToString()
            )
            
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_forwarded_publication(self, ch, method, properties, body):
        pub = messages_pb2.Publication()
        pub.ParseFromString(body)
        
        # This publication was already routed to us by another broker.
        # We only need to match it locally and deliver it.
        # We do NOT forward it again to avoid loops and redundant network traffic.
        self._match_local_and_notify(pub)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        print(f"[{self.broker_id}] Starting Simple Routing Broker...")
        self.start_heartbeat()
        
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.subs', on_message_callback=self.handle_subscription)
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.routing', on_message_callback=self.handle_routing_entry)
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.pubs', on_message_callback=self.handle_publication)
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.forwarded_pubs', on_message_callback=self.handle_forwarded_publication)
        
        print(f"[{self.broker_id}] Waiting for messages.")
        self.channel.start_consuming()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True)
    args = parser.parse_args()
    
    broker = Broker(args.id)
    broker.start()
