import sys
import threading
import argparse
import pika
import time
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.coordinator_client import send_heartbeat, CoordinatorClient
from common.matcher import matches
from proto import messages_pb2

class Broker:
    def __init__(self, broker_id):
        self.broker_id = broker_id
        self.subscriptions = [] # list of messages_pb2.Subscription
        self.seen_publications = set()
        
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
        
        # Subscriptions input queue (from subscribers or other brokers)
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.subs')
        
        # Routed subscriptions (already assigned to this broker)
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.routed_subs')
        
        # Publications input queue (from publishers or other brokers)
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.pubs')
        
        # Notifications exchange
        self.channel.exchange_declare(exchange='notifications_exchange', exchange_type='direct')

    def start_heartbeat(self):
        threading.Thread(target=send_heartbeat, args=(self.broker_id,), daemon=True).start()

    def handle_subscription(self, ch, method, properties, body):
        sub = messages_pb2.Subscription()
        sub.ParseFromString(body)
        
        # hash-based distribution
        brokers = self.coord_client.get_brokers()
        if not brokers:
            # If we don't know the network, keep it locally to be safe
            self.subscriptions.append(sub)
            print(f"[{self.broker_id}] Kept subscription {sub.id} (no other brokers known). Is the coordinator malfunctioning?")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
            
        brokers.sort()
        # hash sub ID to determine target broker
        target_idx = hash(sub.id) % len(brokers)
        target_broker = brokers[target_idx]
        
        if target_broker == self.broker_id:
            # It belongs to us
            self.subscriptions.append(sub)
            # print(f"[{self.broker_id}] Storing routed subscription {sub.id}")
        else:
            # Route it to the responsible broker
            # print(f"[{self.broker_id}] Routing subscription {sub.id} to {target_broker}")
            self.channel.basic_publish(
                exchange='',
                routing_key=f'broker.{target_broker}.routed_subs',
                body=sub.SerializeToString()
            )
            
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_routed_subscription(self, ch, method, properties, body):
        sub = messages_pb2.Subscription()
        sub.ParseFromString(body)
        self.subscriptions.append(sub)
        # print(f"[{self.broker_id}] Received routed subscription {sub.id} from network")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def get_neighbor(self):
        brokers = self.coord_client.get_brokers()
        if not brokers:
            return None
        brokers.sort()
        try:
            idx = brokers.index(self.broker_id)
            neighbor_idx = (idx + 1) % len(brokers)
            neighbor_id = brokers[neighbor_idx]
            if neighbor_id != self.broker_id:
                return neighbor_id
        except ValueError:
            pass
        return None

    def handle_publication(self, ch, method, properties, body):
        pub = messages_pb2.Publication()
        pub.ParseFromString(body)
        
        if pub.id in self.seen_publications:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
            
        self.seen_publications.add(pub.id)
        # print(f"[{self.broker_id}] Processing publication {pub.id}")
        
        # match locally
        match_count = 0
        for sub in self.subscriptions:
            if matches(pub, sub):
                # send notification
                notif = messages_pb2.Notification()
                notif.subscription_id = sub.id
                notif.publication.CopyFrom(pub)
                
                self.channel.basic_publish(
                    exchange='notifications_exchange',
                    routing_key=sub.subscriber_id,
                    body=notif.SerializeToString()
                )
                match_count += 1
                
        # forward to neighbor broker
        neighbor = self.get_neighbor()
        if neighbor:
            # print(f"[{self.broker_id}] Forwarding to {neighbor}")
            self.channel.basic_publish(
                exchange='',
                routing_key=f'broker.{neighbor}.pubs',
                body=pub.SerializeToString()
            )
            
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        print(f"[{self.broker_id}] Starting...")
        self.start_heartbeat()
        
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.subs', on_message_callback=self.handle_subscription)
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.routed_subs', on_message_callback=self.handle_routed_subscription)
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.pubs', on_message_callback=self.handle_publication)
        
        print(f"[{self.broker_id}] Waiting for messages.")
        self.channel.start_consuming()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True)
    args = parser.parse_args()
    
    broker = Broker(args.id)
    broker.start()
