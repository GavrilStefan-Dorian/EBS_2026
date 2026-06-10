import pika
import json
import time
import threading
import os


class Coordinator:
    def __init__(self):
        self.brokers = {}  # broker_id -> last_seen_time
        self.lock = threading.Lock()
        self.topology = self.load_topology()
        self.timeout = self.topology.get("heartbeat_timeout_seconds", 10)

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

        self.channel.queue_declare(queue='coordinator.heartbeats', exclusive=False)
        self.channel.queue_bind(
            exchange='coordinator_exchange',
            queue='coordinator.heartbeats',
            routing_key='heartbeat'
        )

        self.channel.queue_declare(queue='coordinator.rpc_queue')

    def load_topology(self):
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "config",
            "topology.json"
        )

        with open(config_path, "r") as f:
            data = json.load(f)

        # Normalize neighbors as sets/lists later.
        return data

    def get_active_brokers_locked(self):
        now = time.time()
        configured = set(self.topology.get("brokers", []))

        return sorted([
            b_id
            for b_id, last_seen in self.brokers.items()
            if b_id in configured and now - last_seen < self.timeout
        ])

    def compute_active_neighbors(self, broker_id):
        active = self.get_active_brokers_locked()
        active_set = set(active)

        if broker_id not in active_set:
            return []

        configured_neighbors = self.topology.get("neighbors", {})

        graph = {b: set() for b in active}

        # Keep only configured edges whose endpoints are alive.
        for b in active:
            for n in configured_neighbors.get(b, []):
                if n in active_set and n != b:
                    graph[b].add(n)
                    graph[n].add(b)

        # Recovery strategy: if removing a broker disconnects the alive graph,
        # connect components deterministically by representative id.
        if self.topology.get("recovery_strategy") == "connect_components":
            components = self.connected_components(graph)

            if len(components) > 1:
                reps = [sorted(component)[0] for component in components]
                reps = sorted(reps)

                for a, b in zip(reps, reps[1:]):
                    graph[a].add(b)
                    graph[b].add(a)

        return sorted(graph.get(broker_id, []))

    def connected_components(self, graph):
        seen = set()
        components = []

        for node in sorted(graph.keys()):
            if node in seen:
                continue

            stack = [node]
            component = set()
            seen.add(node)

            while stack:
                current = stack.pop()
                component.add(current)

                for nxt in graph[current]:
                    if nxt not in seen:
                        seen.add(nxt)
                        stack.append(nxt)

            components.append(component)

        return components

    def heartbeat_callback(self, ch, method, properties, body):
        data = json.loads(body)
        broker_id = data.get('broker_id')

        if broker_id:
            with self.lock:
                self.brokers[broker_id] = time.time()

    def rpc_callback(self, ch, method, properties, body):
        try:
            request = json.loads(body.decode() if isinstance(body, bytes) else body) if body else {}
        except Exception:
            request = {}

        request_type = request.get("type", "get_brokers")

        with self.lock:
            if request_type == "get_neighbors":
                broker_id = request.get("broker_id")
                response = {
                    "neighbors": self.compute_active_neighbors(broker_id),
                    "active_brokers": self.get_active_brokers_locked()
                }
            else:
                response = self.get_active_brokers_locked()

        ch.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(correlation_id=properties.correlation_id),
            body=json.dumps(response)
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
        self.channel.basic_consume(
            queue='coordinator.heartbeats',
            on_message_callback=self.heartbeat_callback,
            auto_ack=True
        )
        self.channel.basic_consume(
            queue='coordinator.rpc_queue',
            on_message_callback=self.rpc_callback
        )

        monitor_thread = threading.Thread(target=self.monitor_brokers, daemon=True)
        monitor_thread.start()

        print("[Coordinator] Waiting for heartbeats and requests.")
        self.channel.start_consuming()


if __name__ == '__main__':
    coord = Coordinator()
    coord.start()