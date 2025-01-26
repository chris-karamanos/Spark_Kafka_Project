from confluent_kafka import Producer
import json
import time

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:29092,localhost:29093',  # External ports for brokers
    'client.id': 'bike-station-producer'
}

# Initialize the producer
producer = Producer(conf)

# Callback function to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Load bike station data from the JSON file
def load_bike_station_data(filepath):
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data

# Function to send bike station data to the Kafka topic
def send_bike_station_data(filepath):
    bike_station_data_list = load_bike_station_data(filepath)

    for station_data in bike_station_data_list:
        try:
            # Serialize the data as JSON
            serialized_data = json.dumps(station_data)

            # Produce the message to the bike_station_data topic
            producer.produce(
                topic='bike_station_data',
                key=str(station_data.get('station_id')),  #Partitioning key
                value=serialized_data,
                callback=delivery_report
            )

            # Flush to ensure the message is sent
            producer.flush()

            print(f"Sent data: {station_data}")

            # Wait a moment before sending the next message (optional)
            time.sleep(1)

        except Exception as e:
            print(f"An error occurred: {e}")
            break

# Start the producer
if __name__ == "__main__":
    file = "../../decentralized/bike_station_data.json"
    print("Starting bike station data producer...")
    send_bike_station_data(file)
