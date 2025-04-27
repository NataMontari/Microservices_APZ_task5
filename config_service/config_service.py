import configparser
import argparse
from flask import Flask, request, jsonify

app = Flask(__name__)

service_registry = {
}

def load_config(config_file):
    global service_registry
    with open(config_file, "r") as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                ports = [port.strip() for port in value.split(",")]
                service_registry[key] = [f"localhost:{port}" for port in ports]


@app.route("/get_services", methods=["GET"])
def get_services():
    service_name = request.args.get("service")
    if service_name in service_registry:
        return jsonify({"instances": service_registry[service_name]})
    return jsonify({"error": "Service not found"}), 404

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Config Server")
    parser.add_argument("--config", type=str, default="config.txt", help="Path to config file")
    args = parser.parse_args()

    config_file = args.config
    load_config(config_file)
    print(f"Config server running with config: {args.config}")
    
    app.run(port=9000)