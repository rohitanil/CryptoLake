import os
import time
import json
from kafka import KafkaProducer
from src.producer.weather import get_weather_data
from src.utilities.enums import Constants
from src.utilities.utilities import on_send_error, on_send_success


def kafka_producer(secret_token):
    states = Constants.STATES.value
    bootstrap = Constants.BOOTSTRAP_SERVER.value
    topic = Constants.KAFKA_TOPIC.value
    interval = Constants.INTERVAL.value

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        retries=10,  # Retry up to 10 times
        request_timeout_ms=60000,  # Timeout for each request (increase to 60 seconds)
        linger_ms=100,  # Optional: You can reduce the batching delay
        batch_size=16384,  # Increase batch size if needed
        acks='all'
    )
    while True:
        for state in states:
            output = get_weather_data(state, secret_token)
            print(output)
            future = producer.send(topic, value=output)
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)
            time.sleep(interval)


if __name__ == "__main__":
    try:
        token = os.getenv("WEATHER_API_TOKEN")
        kafka_producer(token)
    except Exception as e:
        print(e)
