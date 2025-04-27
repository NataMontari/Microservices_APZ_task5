from flask import Flask, request, jsonify
import grpc
import uuid
import requests
import logging
from proto import logging_pb2
from proto import logging_pb2_grpc
import random
from tenacity import retry, stop_after_attempt, wait_exponential
from kafka import KafkaProducer
import json
from consul import Consul
import sys

app = Flask(__name__)
consul = Consul()

config_service = "http://localhost:9000/get_services"

def get_config(key, default=None):
    index, data = consul.kv.get(key)
    if data:
        return data['Value'].decode('utf-8')
    return default

def get_kafka_config():
    kafka_hosts = get_config('config/message-queue/hosts', 'localhost:9092')
    return kafka_hosts.split(',')

def init_kafka_producer():
    hosts = get_kafka_config()
    return KafkaProducer(
        bootstrap_servers=hosts,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

producer = init_kafka_producer()


def get_kafka_topic():
    return get_config('config/message-queue/topic', 'messages')


def send_to_kafka(topic, message):
    logging.info(f"Sending message to Kafka topic '{topic}': {message}")
    producer.send(topic, message)

def get_services_consule(service_name):
    try:
        index, services = consul.catalog.service(service_name)
        return [f"{service['Address']}:{service['ServicePort']}" for service in services]
    except Exception as e:
        logging.error(f"Error connecting to Consul: {e}")
        return []

def get_service_address(service_name):
    index, services = consul.catalog.service(service_name)
    if not services:
        logging.error(f"No instances of {service_name} found in Consul")
        return []
    
    return [f"{service['ServiceAddress'] or service['Address']}:{service['ServicePort']}" for service in services]

def get_services(service_name):
    try:
        response = requests.get(config_service, params = {"service": service_name})
        if response.status_code == 200:
            return response.json().get("instances", [])
        return []
    except requests.RequestException as e:
        logging.error("Couldn't connect to the config service")
        return []
    
def get_logging_service():
    instances = get_service_address("logging-service")
    if not instances:
        raise Exception("No available instances of logging-service found in Consul")
    return instances

def get_messages_service():
    instances = get_service_address("messages-service")
    if not instances:
        raise Exception("No available instances of messages-service found in Consul")
    return instances

def randomConnect(port = False):
    logging_services = get_logging_service()
    random.shuffle(logging_services)
    for service in logging_services:
        try:
            channel = grpc.insecure_channel(service)
            stub = logging_pb2_grpc.LoggingServiceStub(channel)
            stub.GetMessages(logging_pb2.Empty())
            if port == True:
                return stub, service
            return stub
        except Exception:
            continue
    raise Exception("No available logging services!")


# def randomConnectMessages(port = False):
#     messages_services = get_messages_service()
#     random.shuffle(messages_services)
#     for service in messages_services:
#         try:
#             channel = grpc.insecure_channel(service)
#             stub = logging_pb2_grpc.LoggingServiceStub(channel)
#             stub.GetMessages(logging_pb2.Empty())
#             if port == True:
#                 return stub, service
#             return stub
#         except Exception:
#             continue
#     raise Exception("No available messages services!")


# channel = grpc.insecure_channel("localhost:8081")
# stub = logging_pb2_grpc.LoggingServiceStub(channel)

def get_messages_service_response():
    services = get_messages_service()
    random.shuffle(services)
    for service in services:
        try:
        # Request to messages_service
            response = requests.get(f"http://{service}/get_message")
            logging.info(f"Received response from messages service at {service}: {response.text}")
            return response.text  # Returns a text answer
        except requests.RequestException as e:
            logging.info(f"Service at {service} failed, trying next...")
            continue
    return "All messages-service instances failed to respond"

    
    
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def log_message_with_retry(message_uuid: str, message: str):
    try:
        print(f"Attempting to send message: {message}")
        
        stub, port = randomConnect(port = True)
        request = logging_pb2.LogRequest(id=message_uuid, message=message)
        response = stub.LogMessage(request)  # call gRPC
        
        # logging.info(f"Sent message to logging service at {port}, response: {response.status}")

        if response.status == "Message logged successfully":
            print(f" Message successfully logged: {response.status}")
            return {"status": "success", "message": response.status}
        elif response.status == "Duplicate message ignored":
            print(f"Duplicate send: {response.status}")
            return {"status": "duplicate", "message": response.status}
        else:
            print(f"Attempt failed: {response.status}")
            return {"status": "failed", "message": response.status}

    except grpc.RpcError as e:
        print(f"[gRPC error: {e.code().name} - {e.details()}")
        raise  # repeat the call

@app.route("/send_message", methods=["POST"])
def handle_post():
    msg = request.json.get("message")
    if not msg:
        return jsonify(error="No message provided"), 400
    
    msg_id = str(uuid.uuid4())  # Generate unique uuid
    logging.info(f"Received message: {msg}")


    # For messages service



    try:

        send_to_kafka('messages', msg) # send message to Kafka "queue"

        # send to logging
        response = log_message_with_retry(msg_id, msg)  # Calling function with retry
        return jsonify(id=msg_id, **response)  # Returning the response
    
    except grpc.RpcError as e:
        logging.error(f"Failed to send message: {e}")
        return jsonify(error=f"Logging service error: {e}"), 500

    
@app.route("/get_messages", methods = ["GET"])
def handle_get():
    try:
        stub, port = randomConnect(port = True)
        response = stub.GetMessages(logging_pb2.Empty())
        logging_messages = list(response.messages)

        messages_service_response = get_messages_service_response()

        combined_response = {"logged_messages": logging_messages, "message_from_service": messages_service_response, "port": port} 

        # logging.info(f"Retrieved messages from logging service at {stub}")
        return jsonify(combined_response)
    except grpc.RpcError as e:
        return jsonify(error=f"Service error: {e}"), 500
    except Exception as e:
        app.logger.error(f"Unexpected error: {e}")
        return jsonify(error=f"Unexpected error: {e}"), 500


def register_with_consul(port):
    service_id = f"facade-service-{port}"
    consul.agent.service.register(
        "facade-service",
        service_id=service_id,
        address="localhost",
        port=port,
        tags=["flask"],
        check={
            'http': f'http://localhost:{port}/health',
            'interval': '10s'
        }
    )
    print(f"Registered facade-service with Consul on port {port}")


if __name__ == "__main__":
    port = int(sys.argv[1])
    register_with_consul(port) 
    app.run(port=port)