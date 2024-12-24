from enum import Enum


class KafkaConstants(Enum):
    CONFIG = "/Users/rohitanilkumar/Syracuse/Projects/CryptoLake/src/config/config.yml"
    CRYPTO_URL = "https://min-api.cryptocompare.com/data/pricemulti?"
    KAFKA_TOPIC = "crypto"
    INTERVAL = 1
    CRYPTO = "BTC,ETH"
    BASE_CURRENCY = "INR,USD,EUR"
    BOOTSTRAP_SERVER = "localhost:9092"


