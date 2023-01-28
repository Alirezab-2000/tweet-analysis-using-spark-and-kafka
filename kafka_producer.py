import time

import tweepy
import json
from kafka import KafkaProducer
from configparser import ConfigParser

from pyspark.shell import spark
from pyspark.sql import SparkSession

import auth_tokens

bearer_token = """AAAAAAAAAAAAAAAAAAAAADEJjQEAAAAAQoEyuMUYg%2F2rjF8TJ%2BPVzXqrnrE%3DRw7FLXOIl24UoQ2uJ1l0bC6fKIUJwuPmZNDEp7FQIezkZtr7d5"""
topic_name = 'twitter'

tweet_count = 0


class StreamClient(tweepy.StreamingClient):

    def set_data(self, kafka_producer, topic_name):
        self.producer = kafka_producer
        self.topic_name = topic_name
        self.count = 0

    def on_data(self, raw_data):
        print(self.count)
        self.count += 1
        if self.count >= 50:
            self.disconnect()
            return False

        data_js = json.loads(raw_data)
        data_l = data_js['data']
        self.producer.send(topic_name, value=data_l)

        return True

    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

    def on_status(self, status):
        print(status.text)

# """
if __name__ == "__main__":
    config = ConfigParser()
    config.read("hashtags.conf")
    print(config)

    bootstap_server = config['Kafka_param']['bootstrap.servers']
    kafka_producer = KafkaProducer(bootstrap_servers=[bootstap_server],
                                   value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    streamer = StreamClient(bearer_token)
    streamer.set_data(kafka_producer, topic_name=topic_name)
    auth = tweepy.OAuthHandler(auth_tokens.consumer_key, auth_tokens.consumer_secret)
    auth.set_access_token(auth_tokens.access_token, auth_tokens.access_secret)
    # Converting string to float to get cordinates
    streamer.add_rules(tweepy.StreamRule("worldcup"))
    streamer.filter()
# """
