from flask import Flask, request, jsonify
import sys
from kafka import KafkaConsumer
import json
from consul import Consul

consul = Consul()

app = Flask(__name__)

received_messages = [""]

def get_config(key, default=None):
    consul_client = Consul()
    index, data = consul_client.kv.get(key)
    if data:
        return data['Value'].decode('utf-8')
    return default


def get_kafka_config():
    hosts = get_config('config/message-queue/hosts', 'localhost:9092')
    topic = get_config('config/message-queue/topic', 'messages')
    group_id = get_config('config/message-queue/group-id', 'messages-group')
    
    return {
        'hosts': hosts.split(','),
        'topic': topic,
        'group_id': group_id
    }

def init_kafka_consumer():
    kafka_config = get_kafka_config()
    return KafkaConsumer(
        kafka_config['topic'],
        bootstrap_servers=kafka_config['hosts'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=kafka_config['group_id'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

consumer = init_kafka_consumer()


@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "up"}), 200

def register_with_consul(port):
    consul = Consul()
    service_id = f"messages-service-{port}"
    consul.agent.service.register(
        "messages-service",
        service_id=service_id,
        address="localhost",
        port=port,
        tags=["flask"],
        check={
            'http': f'http://localhost:{port}/health',
            'interval': '10s'
        }
    )
    print(f"Registered messages-service with Consul on port {port}")

def consume_messages():
    # try:
    while True:
            # Explicitly poll messages
        msg_pack = consumer.poll(timeout_ms=1000)
        print(msg_pack)
        if msg_pack:
            for topic_partition, messages in msg_pack.items():
                for message in messages:
                    print(f"Consumed message: {message.value}")
                    received_messages.append(message.value)
        if msg_pack == {}:
            break
        


    # except Exception as e:
    #     print(f"Error consuming messages: {e}")


    return 0

@app.route("/get_message", methods=["GET"])
def get_message():
    consume_messages()
    return jsonify({
        "port": port,
        "messages": received_messages
    }), 200


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception("Please enter a port number from 8082 to 8084")
    port = int(sys.argv[1])  # Перетворюємо на int
    if port < 8085 or port > 8089:
       raise Exception("Please enter a port number from 8082 to 8084")
    register_with_consul(port)
    print(f"Running messages-service on port {port}")
    app.run(port=port)
