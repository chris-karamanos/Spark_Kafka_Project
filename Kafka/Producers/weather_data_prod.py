from confluent_kafka import Producer
import json
import time
import random

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:29092,localhost:29093',  
    'client.id': 'weather-data-producer',   # Unique ID for this producer
}

# Initialize the producer
producer = Producer(conf)

# Callback function to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Load weather data from the JSON file
def load_weather_data(filepath):
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data

# Function to send weather data to the Kafka topic
def send_weather_data(filepath):
    weather_data_list = load_weather_data(filepath)

    for weather_data in weather_data_list:
        try:
            # Serialize the data as JSON
            serialized_data = json.dumps(weather_data)

            # Produce the message to the weather_data topic
            producer.produce(
                topic='weather_data',
                key=str(weather_data.get('timestamp')),  # Optional: Partitioning key
                value=serialized_data,
                callback=delivery_report
            )

            # Flush to ensure the message is sent
            producer.flush()

            print(f"Sent data: {weather_data}")

            # Wait a moment before sending the next message (optional)
            time.sleep(1)

        except Exception as e:
            print(f"An error occurred: {e}")
            break

# Start the producer
if __name__ == "__main__":
    file = "../../decentralized/weather_data.json"
    print("Starting weather data producer...")
    send_weather_data(file)
