import tweepy
import json
from kafka import KafkaProducer

import auth_tokens

topic_name = 'twitter'


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


if __name__ == "__main__":
    bootstrap_server = "localhost:9092"
    kafka_producer = KafkaProducer(bootstrap_servers=[bootstrap_server],
                                   value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    streamer = StreamClient(auth_tokens.bearer_token)
    streamer.set_data(kafka_producer, topic_name=topic_name)
    auth = tweepy.OAuthHandler(auth_tokens.consumer_key, auth_tokens.consumer_secret)
    auth.set_access_token(auth_tokens.access_token, auth_tokens.access_secret)

    streamer.sample()
