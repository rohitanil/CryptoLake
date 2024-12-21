import yaml
import time
import json
from kafka import KafkaProducer
from src.producer.crypto import get_coin_price
from enums import Constants
from utilities import on_send_error, on_send_success

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


def config_reader(path):
    try:
        with open(path, 'r') as stream:
            dictionary = yaml.safe_load(stream)
            if dictionary is not None:
                secret = dictionary.get("crypto").get("api_token")
                return secret
            else:
                print("YAML file is empty or invalid.")
    except FileNotFoundError:
        print("The specified YAML file was not found.")
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")


def kafka_producer(secret_token):
    crypto = Constants.CRYPTO.value
    base_currencies = Constants.BASE_CURRENCY.value
    bootstrap = Constants.BOOTSTRAP_SERVER.value
    topic = Constants.KAFKA_TOPIC.value
    interval = Constants.INTERVAL.value

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
        token = config_reader(Constants.CONFIG.value)
        kafka_producer(token)
    except Exception as e:
        print(e)
