import time

import tweepy
import json
from kafka import KafkaProducer
from configparser import ConfigParser

from pyspark.shell import spark
from pyspark.sql import SparkSession

import auth_tokens

# kafka topic name
topic_name = 'twitter'


# Create a class for use Twitter Client Service(API)
class StreamClient(tweepy.StreamingClient):

    def set_data(self, kafka_producer, topic_name):
        self.producer = kafka_producer
        self.topic_name = topic_name
        self.count = 0

    # we handle data here
    def on_data(self, raw_data):
        print(self.count)
        print(raw_data)
        self.count += 1
        if self.count >= 400:  # for skip from ban of API, wee restrict input data length
            self.disconnect()
            return False

        data_js = json.loads(raw_data)  # we convert data to json for serializer on kafka
        data_l = data_js['data']
        self.producer.send(topic_name, value=data_l)
        time.sleep(0.1)

        return True

    # we notify from connection errors here
    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

    # we notify from status of the connection
    def on_status(self, status):
        print(status.text)


def get_data_from_twitter_and_send():
    ONLINE_MODE = True  # if you can not fetch data from Twitter, set ONLINE_MODE = False
    if ONLINE_MODE:
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
    else:
        kafka_producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                       value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        i = 0
        while (i <= 50):
            # this is a fake data
            raw_data = {"data": {"edit_history_tweet_ids": ["1594712968507293696"], "id": "1594712968507293696",
                                 "text": "@Palm_angelss #hi #iran #11"},
                        "matching_rules": [{"id": "1593378942894874625", "tag": ""}]}
            data_l = raw_data['data']
            kafka_producer.send(topic_name, value=data_l)
            time.sleep(1)
            i = i + 1
            print(i)


get_data_from_twitter_and_send()

