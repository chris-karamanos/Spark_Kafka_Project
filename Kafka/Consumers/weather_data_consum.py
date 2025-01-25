from confluent_kafka import Consumer, KafkaException

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:29092,localhost:29093',  
    'group.id': 'weather-data-group',        # Consumer group ID
    'auto.offset.reset': 'earliest'         # Start from the earliest message
}

# Initialize the consumer
consumer = Consumer(conf)

try:
    print("Subscribing to topics...")
    consumer.subscribe(['weather_data'])
    print("Subscription successful. Starting to poll messages...")
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for new messages
        if msg is None:
            continue  # No message received, continue polling
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # Reached the end of a partition
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        # Print the received message
        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Exiting...")

finally:
    # Close the consumer to free resources
    consumer.close()
