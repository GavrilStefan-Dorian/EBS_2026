import pika
import json
import uuid
import time

class CoordinatorClient:
    def __init__(self):
        retries = 15
        while retries > 0:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
                break
            except pika.exceptions.AMQPConnectionError:
                print(f"[CoordinatorClient] Waiting for RabbitMQ... ({retries} retries left)")
                retries -= 1
                time.sleep(2)
        else:
            raise Exception("Could not connect to RabbitMQ")
                
        self.channel = self.connection.channel()
        
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        self.response = None
        self.corr_id = None
        
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = json.loads(body)

    def get_brokers(self):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='coordinator.rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=''
        )
        self.connection.process_data_events(time_limit=2)
        return self.response or []

    def close(self):
        self.connection.close()

def send_heartbeat(broker_id):
    retries = 15
    while retries > 0:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
            break
        except pika.exceptions.AMQPConnectionError:
            retries -= 1
            time.sleep(2)
    else:
        print(f"[Heartbeat] Failed to connect to RabbitMQ for {broker_id}")
        return
            
    channel = connection.channel()
    channel.exchange_declare(exchange='coordinator_exchange', exchange_type='direct')
    
    while True:
        try:
            channel.basic_publish(
                exchange='coordinator_exchange',
                routing_key='heartbeat',
                body=json.dumps({"broker_id": broker_id})
            )
            connection.sleep(5)
        except pika.exceptions.ConnectionClosedByBroker:
            break
        except KeyboardInterrupt:
            break
