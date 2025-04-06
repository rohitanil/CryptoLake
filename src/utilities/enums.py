from enum import Enum


class Constants(Enum):
    WEATHER_URL = "http://api.weatherapi.com/v1/current.json?key={key}&q={state}&aqi=yes"
    KAFKA_TOPIC = "weather"
    INTERVAL = 10
    STATES = ["San Francisco", "New York"]
    BOOTSTRAP_SERVER = "localhost:29092"



