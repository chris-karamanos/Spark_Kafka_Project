from confluent_kafka import Consumer

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:29092,localhost:29093',  # External ports for brokers
    'group.id': 'bike-station-group',  # Unique consumer group ID
    'auto.offset.reset': 'earliest'   # Start from the earliest message
}

# Initialize the consumer
consumer = Consumer(conf)

# Subscribe to the bike_station_data topic
consumer.subscribe(['bike_station_data'])

try:
    print("Listening for messages on 'bike_station_data'...")
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for new messages
        if msg is None:
            continue  # No message received
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Print the received message
        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Exiting...")

finally:
    # Close the consumer to free resources
    consumer.close()
