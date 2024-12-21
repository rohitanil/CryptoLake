from enum import Enum


class Constants(Enum):
    CONFIG = "/Users/rohitanilkumar/Syracuse/Projects/Reddit-Kafka-Pinot-Superset/src/config/config.yml"
    CRYPTO_URL = "https://min-api.cryptocompare.com/data/pricemulti?"
    KAFKA_TOPIC = "crypto"
    INTERVAL = 5
    CRYPTO= "BTC,ETH"
    BASE_CURRENCY= "INR,USD,EUR"
    BOOTSTRAP_SERVER= "localhost:9092"
