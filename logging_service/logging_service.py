import grpc
from concurrent import futures
import logging
import hazelcast
from proto import logging_pb2
from proto import logging_pb2_grpc
import sys
from consul import Consul

consul = Consul()

def get_config(key, default=None):
    consul_client = Consul()
    index, data = consul_client.kv.get(key)
    if data:
        return data['Value'].decode('utf-8')
    return default

# Реєстрація сервісу в Consul
def register_service(service_name, port):
    consul = Consul()
    service_id = f"{service_name}-{port}"
    
    # Реєстрація сервісу з HTTP health check
    consul.agent.service.register(
        service_name,
        service_id=service_id,
        address='localhost',
        port=port,
        tags=['grpc'],
        check={
            'http': f'http://localhost:{port}/health',
            'interval': '10s'
        }
    )
    print(f"Registered {service_name} with Consul on port {port}")

def get_hazelcast_config():
    instance_name = get_config('config/hazelcast/instance-name', 'hz-instance')
    members = get_config('config/hazelcast/network/join/tcp-ip/members', 'localhost')
    port = get_config('config/hazelcast/network/port', '5701')
    
    config = hazelcast.ClientConfig()
    config.cluster_name = instance_name
    for member in members.split(','):
        config.network_config.addresses.append(f"{member}:{port}")
    
    return config


class LoggingService(logging_pb2_grpc.LoggingServiceServicer):
    def __init__(self, port):
        hz_config = get_hazelcast_config()
        self.hz_client = hazelcast.HazelcastClient(hz_config)
        self.message_map = self.hz_client.get_map(f"messages_{port}").blocking()
    
    def LogMessage(self, request, context):

        if request.message in self.message_map.values():
            return logging_pb2.LogResponse(status="Duplicate message ignored")

        self.message_map.put(request.id, request.message)
        return logging_pb2.LogResponse(status="Message logged successfully")
    
    def GetMessages(self, request, context):
        return logging_pb2.MessagesResponse(messages = list(self.message_map.values()))
    
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "up"}), 200
    
def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers = 10))
    logging_pb2_grpc.add_LoggingServiceServicer_to_server(LoggingService(port), server)
    server.add_insecure_port(f'[::]:{port}')

    logging.info(f"Starting gRPC server on port {port}...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 2:
        raise Exception("Please enter a port number from 8082 to 8084")
    port = int(sys.argv[1]) 
    register_service('logging-service', port)
    serve(port)