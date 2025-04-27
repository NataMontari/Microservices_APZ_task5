#!/bin/bash

# Send POST request
echo -e "\nSending POST request..."
curl -X POST http://localhost:8080/send_message -H "Content-Type: application/json" -d '{"message": "Test message 10"}'