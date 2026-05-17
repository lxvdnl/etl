from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("KafkaReader").getOrCreate()

password = spark.conf.get("spark.kafka.password")

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers",
            "rc1a-9g5eimm2olsdu0c9.mdb.yandexcloud.net:9091") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.scram.ScramLoginModule required '
            f'username="kafka-user" password="{password}";') \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

df.select(col("value").cast("string").alias("message")).show(truncate=False)
