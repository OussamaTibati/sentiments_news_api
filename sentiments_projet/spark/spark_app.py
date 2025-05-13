from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

spark = SparkSession.builder.appName("NewsSentimentAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType().add("text", StringType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news_topic") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.text")

def get_sentiment(text):
    return TextBlob(text).sentiment.polarity

sentiment_udf = udf(get_sentiment, FloatType())
scored = parsed.withColumn("sentiment", sentiment_udf(col("text")))

query = scored.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
