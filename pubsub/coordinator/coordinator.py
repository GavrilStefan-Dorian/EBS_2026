import pika
import json
import time
import threading

# Coordinator runs and listens for heartbeats
# and registration requests.
# It maintains a list of alive brokers.

class Coordinator:
    def __init__(self):
        self.brokers = {} # broker_id -> last_seen_time
        self.lock = threading.Lock()
        self.timeout = 10 # seconds

        # Set up RabbitMQ connection with retries
        retries = 15
        while retries > 0:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                break
            except pika.exceptions.AMQPConnectionError:
                print(f"[Coordinator] Waiting for RabbitMQ to become available... ({retries} retries left)")
                retries -= 1
                time.sleep(2)
        else:
            raise Exception("Could not connect to RabbitMQ")
                
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='coordinator_exchange', exchange_type='direct')
        
        # Queue for heartbeats
        result = self.channel.queue_declare(queue='coordinator.heartbeats', exclusive=False)
        self.channel.queue_bind(exchange='coordinator_exchange', queue='coordinator.heartbeats', routing_key='heartbeat')

        # Queue for RPC requests (get brokers)
        self.channel.queue_declare(queue='coordinator.rpc_queue')

    def heartbeat_callback(self, ch, method, properties, body):
        data = json.loads(body)
        broker_id = data.get('broker_id')
        if broker_id:
            with self.lock:
                self.brokers[broker_id] = time.time()
            # print(f"[Coordinator] Heartbeat from broker {broker_id}")

    def rpc_callback(self, ch, method, properties, body):
        # Return list of active brokers
        with self.lock:
            active_brokers = [b_id for b_id, last_seen in self.brokers.items() if time.time() - last_seen < self.timeout]
        response = json.dumps(active_brokers)
        
        ch.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
            body=response
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def monitor_brokers(self):
        while True:
            time.sleep(5)
            now = time.time()
            with self.lock:
                dead_brokers = []
                for b_id, last_seen in list(self.brokers.items()):
                    if now - last_seen > self.timeout:
                        dead_brokers.append(b_id)
                
                for b_id in dead_brokers:
                    print(f"[Coordinator] Broker {b_id} timeout.")
                    del self.brokers[b_id]

    def start(self):
        print("[Coordinator] Starting...")
        self.channel.basic_consume(queue='coordinator.heartbeats', on_message_callback=self.heartbeat_callback, auto_ack=True)
        self.channel.basic_consume(queue='coordinator.rpc_queue', on_message_callback=self.rpc_callback)

        monitor_thread = threading.Thread(target=self.monitor_brokers, daemon=True)
        monitor_thread.start()

        print("[Coordinator] Waiting for heartbeats and requests.")
        self.channel.start_consuming()

if __name__ == '__main__':
    coord = Coordinator()
    coord.start()
