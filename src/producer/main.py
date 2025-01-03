import time
import json
from kafka import KafkaProducer
from src.producer.crypto import get_coin_price
from src.utilities.enums import KafkaConstants
from src.utilities.utilities import on_send_error, on_send_success, config_reader


def kafka_producer(secret_token):
    crypto = KafkaConstants.CRYPTO.value
    base_currencies = KafkaConstants.BASE_CURRENCY.value
    bootstrap = KafkaConstants.BOOTSTRAP_SERVER.value
    topic = KafkaConstants.KAFKA_TOPIC.value
    interval = KafkaConstants.INTERVAL.value

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        retries=5
    )
    while True:
        output = get_coin_price(crypto, base_currencies, secret_token)
        future = producer.send(topic, value=output)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
        time.sleep(interval)


if __name__ == "__main__":
    try:
        token = config_reader(KafkaConstants.CONFIG.value)
        kafka_producer(token)
    except Exception as e:
        print(e)
