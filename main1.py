import json
import os
import sys
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import *
from pyspark.sql import functions as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import tensorflow as tf

kafka_producer2 = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
kafka_classification_producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
kafka_emotion_producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))



classificationTokenizer = AutoTokenizer.from_pretrained("jonaskoenig/topic_classification_04")
classificationModel = AutoModelForSequenceClassification.from_pretrained("jonaskoenig/topic_classification_04", from_tf=True)
classificationPipline = pipeline('text-classification', model=classificationModel, tokenizer=classificationTokenizer)

# emotionTokenizer = AutoTokenizer.from_pretrained("j-hartmann/emotion-english-distilroberta-base")
# emotionModel = AutoModelForSequenceClassification.from_pretrained("j-hartmann/emotion-english-distilroberta-base")
# emotionPipline = pipeline("text-classification", model=emotionModel, tokenizer=emotionTokenizer)

# Write to mongoDB in batches
def write_row(batch_df, batch_id):
    # print(1111, batch_df)
    data = batch_df.toPandas().set_index('word').to_dict()["count"]
    first_10_pairs = {k: data[k] for k in list(data)[:10]}
    print(first_10_pairs)
    kafka_producer2.send("t1", "hashtag_result@@"+str(first_10_pairs))
    batch_df.write.format("console").mode("append").save()
    pass

def write_emotion_row(batch_df, batch_id):
    # print(1111, batch_df)
    data = batch_df.toPandas().set_index('emotion_result').to_dict()["count"]
    first_10_pairs = {k: data[k] for k in list(data)[:7]}
    print(first_10_pairs)
    kafka_emotion_producer.send("emotion", "emotion_result@@"+str(first_10_pairs))
    batch_df.write.format("console").mode("append").save()
    pass

def write_classification_row(batch_df, batch_id):
    data = batch_df.toPandas().set_index('classification_result').to_dict()["count"]
    first_10_pairs = {k: data[k] for k in list(data)[:10]}
    print(first_10_pairs)
    kafka_emotion_producer.send("classification", "classification_result@@"+str(first_10_pairs))
    batch_df.write.format("console").mode("append").save()
    pass


def is_hashtag(word):
    word = str(word).replace("#_","").replace("# "," ")
    return str(word).__contains__("#")


def create_spark_session(app_name):
    return SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
        .getOrCreate()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0
if __name__ == "__main__":
    # spark = SparkSession \
    #     .builder \
    #     .appName("Sentiment_Analysis_TW") \
    #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    #     .getOrCreate()

    spark = create_spark_session("Sentiment_Analysis_TW")
    # classification_spark = create_spark_session("classification")
    # emotion_spark = create_spark_session("emotion")

    twitter_schema = StructType([
        StructField("text", StringType(), False)])

    twitter_stream_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter") \
        .option("startingOffsets", "latest") \
        .option("max.poll.records", 100) \
        .option("failOnDataLoss", False) \
        .load()
    twitter_records = twitter_stream_df.selectExpr("CAST(key AS STRING)", "CAST(value as STRING) as twitter_data") \
        .select(from_json("twitter_data", schema=twitter_schema).alias("twitter_data"))
    tweeter_data = twitter_records.select("twitter_data")
    tweeter_data = tweeter_data.select("twitter_data.*")
    
    print(44, tweeter_data)

    tweeter_data.printSchema()

    filter_udf = udf(lambda row: [x for x in row if is_hashtag(x)], ArrayType(StringType()))


    df = (
        tweeter_data.withColumn("word", F.explode(filter_udf(F.split(F.col("text"), " "))))
        .withColumn("word", F.regexp_replace("word", "[^\w]", ""))
        .groupBy("word")
        .count()
        .sort("count", ascending=False)
    )

    classification_udf = udf(lambda row: classificationPipline(row)[0]['label'], StringType())

    classification_df = (
        tweeter_data.withColumn("classification_result", classification_udf(F.col("text")))
        .groupBy("classification_result")
        .count()
        .sort("count", ascending=False)
    )

    # emotion_udf = udf(lambda row: emotionPipline(row)[0]['label'], StringType())

    # emotion_df = (
    #     tweeter_data.withColumn("emotion_result", emotion_udf(F.col("text")))
    #     .groupBy("emotion_result")
    #     .count()
    #     .sort("count", ascending=False)
    # )


    hashtag_stram = df.writeStream.outputMode("complete").foreachBatch(write_row).start()
    # emotion_stream = emotion_df.writeStream.outputMode("complete").foreachBatch(write_emotion_row).start()
    classififcation_stream = classification_df.writeStream.outputMode("complete").foreachBatch(write_classification_row).start()

    classififcation_stream.awaitTermination()
    hashtag_stram.awaitTermination()
    # emotion_stream.awaitTermination()

    print("Finish...")
