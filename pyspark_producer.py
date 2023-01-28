from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Sentiment_Analysis_TW") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # raw_data = [
    #     ('James', "Hi"),
    #     ('Robert', "Heloo"),
    #     ('Washington', "3"),
    #     ('Jefferson', "4")
    # ]
    raw_data = [("1", "@Palm_angelss #hi #iran #11")] * 10

    df = spark.createDataFrame(data=raw_data, schema=["id", "text"])
    df.printSchema()
    df.show(truncate=False)

    # df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
    # .writeStream
    # .format("kafka")
    # .outputMode("append")
    # .option("kafka.bootstrap.servers", "192.168.1.100:9092")
    # .option("topic", "josn_data_topic")
    # .start()
    # .awaitTermination()

    # df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .write \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("failOnDataLoss", "false") \
    #     .option("topic", "t1") \
    #     .save()
