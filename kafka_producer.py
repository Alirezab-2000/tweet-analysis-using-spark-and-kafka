import time

import tweepy
import json
from kafka import KafkaProducer
from configparser import ConfigParser

from pyspark.shell import spark
from pyspark.sql import SparkSession

import auth_tokens

topic_name = 'twitter'

tweet_count = 0


class StreamClient(tweepy.StreamingClient):

    def set_data(self, kafka_producer, topic_name):
        print("hi")
        self.producer = kafka_producer
        self.topic_name = topic_name
        self.count = 0

    def on_data(self, raw_data):
        print(self.count)
        print(raw_data)
        self.count += 1
        if self.count >= 400:
            self.disconnect()
            return False

        data_js = json.loads(raw_data)
        data_l = data_js['data']
        self.producer.send(topic_name, value=data_l)
        time.sleep(0.1)

        return True

    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

    def on_status(self, status):
        print(status.text)


"""
if __name__ == "__main__":
    kafka_producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                   value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    i = 0
    while (i <= 50):
        raw_data = {"data": {"edit_history_tweet_ids": ["1594712968507293696"], "id": "1594712968507293696",
                             "text": "@Palm_angelss #hi #iran #11"},
                    "matching_rules": [{"id": "1593378942894874625", "tag": ""}]}
        data_l = raw_data['data']
        kafka_producer.send(topic_name, value=data_l)
        time.sleep(1)
        i = i + 1
        print(i)

"""


# """
if __name__ == "__main__":
    config = ConfigParser()
    config.read("hashtags.conf")
    print(config)

    bootstap_server = "localhost:9092"
    kafka_producer = KafkaProducer(bootstrap_servers=[bootstap_server],
                                   value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    streamer = StreamClient(auth_tokens.bearer_token)
    streamer.set_data(kafka_producer, topic_name=topic_name)
    auth = tweepy.OAuthHandler(auth_tokens.consumer_key, auth_tokens.consumer_secret)
    auth.set_access_token(auth_tokens.access_token, auth_tokens.access_secret)
    streamer.filter()

# """

