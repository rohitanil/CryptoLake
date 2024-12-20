import time

import yaml
import praw
import json
from kafka import KafkaProducer
from enums import Constants

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


def config_reader(path):
    try:
        with open(path, 'r') as stream:
            dictionary = yaml.safe_load(stream)
            if dictionary is not None:
                client = dictionary.get("reddit").get("client")
                secret = dictionary.get("reddit").get("secret")
                return client, secret
            else:
                print("YAML file is empty or invalid.")
    except FileNotFoundError:
        print("The specified YAML file was not found.")
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")


def reddit_agent(c_id, token):
    user_agent = Constants.REDDIT_AGENT.value
    subreddit_name = Constants.SUBREDDIT.value
    kafka_topic = Constants.KAFKA_TOPIC.value
    reddit = praw.Reddit(
        client_id=c_id,
        client_secret=token,
        user_agent=user_agent
    )
    # Kafka Producer initialization
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
    )

    subreddit = reddit.subreddit(subreddit_name)

    def on_send_success(record_metadata):
        print(f"Message successfully sent to {record_metadata.topic} "
              f"partition {record_metadata.partition} "
              f"at offset {record_metadata.offset}")

    def on_send_error(exp):
        print(f"Error sending message: {exp}")

    # Get the 10 most recent posts and push them to Kafka
    for submission in subreddit.new(limit=Constants.LIMIT.value):
        message = {
            'title': submission.title,
            'author': str(submission.author),
            'url': submission.url,
            'created_utc': submission.created_utc,
            'upvote': submission.score
        }
        print(f"Message: {message}")
        # Send the message to the Kafka topic with callback functions
        producer.send(kafka_topic, value=message).add_callback(on_send_success).add_errback(on_send_error)
        print(f"Sent to Kafka: {message}")

    producer.flush()
    producer.close()


if __name__ == '__main__':
    conf_path = Constants.CONFIG.value
    client_id, secret_key = config_reader(conf_path)
    reddit_agent(client_id, secret_key)
