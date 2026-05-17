from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaWriter").getOrCreate()

password = spark.conf.get("spark.kafka.password")

messages = [
    ("message 0",),
    ("message 1",),
    ("message 2",),
    ("message 3",),
    ("message 4",),
    ("message 5",),
    ("message 6",),
    ("message 7",),
    ("message 8",),
    ("message 9",),
]

df = spark.createDataFrame(messages, ["value"])

df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers",
            "rc1a-9g5eimm2olsdu0c9.mdb.yandexcloud.net:9091") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.scram.ScramLoginModule required '
            f'username="kafka-user" password="{password}";') \
    .option("topic", "test-topic") \
    .save()

print("Done! 10 messages sent to test-topic.")
