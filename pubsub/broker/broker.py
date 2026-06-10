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

NEIGHBORS = {
    "b1": ["b2"],
    "b2": ["b1", "b3"],
    "b3": ["b2"],
}

class Broker:
    def __init__(self, broker_id):
        self.local_sub_count = 0
        self.broker_id = broker_id
        
        self.neighbors = []
        self.active_brokers = []
        self.routing_seen = set()
        self.seen_publications = set()
        self.local_subscription_ids = set()
        
        self.local_subscriptions = [] # Subscriptions stored on this broker
        self.routing_table = [] #  Tuples of (message_pb2.Subscription, next_hop_broker_id)
        
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
        self.channel.basic_qos(prefetch_count=100)
        
        # 1. Subscriptions from subscribers
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.subs', durable=True)
        
        # 2. Routing entries from other brokers (flooded subscriptions)
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.routing', durable=True)
        
        # 3. Publications from publishers (entry point)
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.pubs', durable=True)
        
        # 4. Publications forwarded from other brokers
        self.channel.queue_declare(queue=f'broker.{self.broker_id}.forwarded_pubs')
        
        # Notifications exchange
        self.channel.exchange_declare(exchange='notifications_exchange', exchange_type='direct')

    def start_heartbeat(self):
        threading.Thread(target=send_heartbeat, args=(self.broker_id,), daemon=True).start()

    
    def advertise_subscription(self, sub):
        routing_entry = messages_pb2.RoutingEntry()
        routing_entry.source_broker = self.broker_id
        routing_entry.subscription.CopyFrom(sub)

        for neighbor in self.neighbors:
            self.channel.basic_publish(
                exchange='',
                routing_key=f'broker.{neighbor}.routing',
                body=routing_entry.SerializeToString(),
                properties=pika.BasicProperties(delivery_mode=2)
            )

    def reflood_local_subscriptions(self):
        print(f"[{self.broker_id}] Reflooding {len(self.local_subscriptions)} local subscriptions to neighbors {self.neighbors}")
        for sub in self.local_subscriptions:
            self.advertise_subscription(sub)
            
    def handle_subscription(self, ch, method, properties, body):
        sub = messages_pb2.Subscription()
        sub.ParseFromString(body)
        
        if sub.id in self.local_subscription_ids:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        self.local_subscription_ids.add(sub.id)
        self.local_subscriptions.append(sub)
        self.local_sub_count += 1
        
        if self.local_sub_count % 1000 == 0:
            print(f"[{self.broker_id}] Stored {self.local_sub_count} local subscriptions")
                
        self.advertise_subscription(sub)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_routing_entry(self, ch, method, properties, body):
        entry = messages_pb2.RoutingEntry()
        entry.ParseFromString(body)

        incoming_neighbor = entry.source_broker
        sub_id = entry.subscription.id
        
        if sub_id in self.local_subscription_ids:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        seen_key = (sub_id, incoming_neighbor)

        if seen_key in self.routing_seen:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        self.routing_seen.add(seen_key)

        # Store route: to reach this subscription, forward publications to incoming_neighbor.
        self.routing_table.append((entry.subscription, incoming_neighbor))

        # Forward the routing entry to all neighbors except the one it came from.
        forwarded_entry = messages_pb2.RoutingEntry()
        forwarded_entry.source_broker = self.broker_id
        forwarded_entry.subscription.CopyFrom(entry.subscription)

        for neighbor in self.neighbors:
            if neighbor != incoming_neighbor:
                self.channel.basic_publish(
                    exchange='',
                    routing_key=f'broker.{neighbor}.routing',
                    body=forwarded_entry.SerializeToString(),
                    properties=pika.BasicProperties(delivery_mode=2)
                )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _route_publication(self, pub, incoming_broker=None):
        if pub.id in self.seen_publications:
            return

        self.seen_publications.add(pub.id)
        
        
        # Deliver to local subscriptions first.
        self._match_local_and_notify(pub)

        # Then forward to next-hop brokers whose routing entries match.
        target_brokers = set()

        for routing_sub, next_hop in self.routing_table:
            if next_hop == incoming_broker:
                continue

            if matches(pub, routing_sub):
                target_brokers.add(next_hop)

        for target_broker in target_brokers:
            self.channel.basic_publish(
                exchange='',
                routing_key=f'broker.{target_broker}.forwarded_pubs',
                properties=pika.BasicProperties(
                    headers={"from_broker": self.broker_id}
                ),
                body=pub.SerializeToString(),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
    def _match_local_and_notify(self, pub):
        for sub in self.local_subscriptions:
            if matches(pub, sub):
                notif = messages_pb2.Notification()
                notif.subscription_id = sub.id
                notif.publication.CopyFrom(pub)
                
                self.channel.basic_publish(
                    exchange='notifications_exchange',
                    routing_key=sub.subscriber_id,
                    body=notif.SerializeToString(),
                    properties=pika.BasicProperties(delivery_mode=2)
                )

    def handle_publication(self, ch, method, properties, body):
        pub = messages_pb2.Publication()
        pub.ParseFromString(body)

        self._route_publication(pub, incoming_broker=None)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def handle_forwarded_publication(self, ch, method, properties, body):
        pub = messages_pb2.Publication()
        pub.ParseFromString(body)

        incoming_broker = None
        if properties and properties.headers:
            incoming_broker = properties.headers.get("from_broker")

        self._route_publication(pub, incoming_broker=incoming_broker)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        print(f"[{self.broker_id}] Starting Simple Routing Broker...")
        self.start_heartbeat()
        threading.Thread(target=self.refresh_neighbors_loop, daemon=True).start()
        time.sleep(1)


        self.channel.basic_consume(queue=f'broker.{self.broker_id}.subs', on_message_callback=self.handle_subscription)
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.routing', on_message_callback=self.handle_routing_entry)
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.pubs', on_message_callback=self.handle_publication)
        self.channel.basic_consume(queue=f'broker.{self.broker_id}.forwarded_pubs', on_message_callback=self.handle_forwarded_publication)
        
        print(f"[{self.broker_id}] Waiting for messages.")
        self.channel.start_consuming()
        
    def refresh_neighbors_loop(self):
        while True:
            try:
                new_neighbors, active_brokers = self.coord_client.get_neighbors(self.broker_id)

                new_neighbors = sorted(new_neighbors)
                active_brokers = sorted(active_brokers)

                # Guard against transient empty topology while coordinator is converging.
                # If more than one broker is alive, this broker should not become isolated.
                if not new_neighbors and len(active_brokers) > 1:
                    print(
                        f"[{self.broker_id}] Ignoring transient empty neighbor list. "
                        f"Active brokers: {active_brokers}"
                    )
                    time.sleep(2)
                    continue

                if set(new_neighbors) != set(self.neighbors):
                    old_neighbors = list(self.neighbors)

                    # Update immediately so this thread does not schedule
                    # the same topology change repeatedly.
                    self.neighbors = new_neighbors
                    self.active_brokers = active_brokers

                    def apply_update(
                        old_neighbors=old_neighbors,
                        new_neighbors=new_neighbors,
                        active_brokers=active_brokers
                    ):
                        self.apply_neighbors_update(
                            old_neighbors,
                            new_neighbors,
                            active_brokers
                        )

                    self.connection.add_callback_threadsafe(apply_update)
                else:
                    self.active_brokers = active_brokers

            except Exception as e:
                print(f"[{self.broker_id}] Could not refresh neighbors: {e}")

            time.sleep(2)

    def apply_neighbors_update(self, old_neighbors, new_neighbors, active_brokers):
        active_set = set(active_brokers)

        before = len(self.routing_table)
        self.routing_table = [
            (sub, next_hop)
            for sub, next_hop in self.routing_table
            if next_hop in active_set
        ]
        after = len(self.routing_table)

        print(
            f"[{self.broker_id}] Topology updated: {old_neighbors} -> {new_neighbors}. "
            f"Removed {before - after} stale routing entries."
        )

        self.reflood_local_subscriptions()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True)
    args = parser.parse_args()
    
    broker = Broker(args.id)
    broker.start()
