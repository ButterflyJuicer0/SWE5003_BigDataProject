from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
from config.kafka_config import kafka_configs
from config.es_config import es_configs

class MemberDataProcess:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("MemberDataProcess") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3") \
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
            .getOrCreate()

        self.extra_kafka_configs = {
            'subscribe': 'member_topic',
            'kafka.group.id': 'member_consumer',
        }

        self.extra_es_configs = {
            'es.resource': 'member_data/_doc'
        }

        self.schema = StructType().add("timestamp", StringType()) \
            .add("event", StringType()) \
            .add("user_name", StringType()) \
            .add("description", StringType())

    def read_from_kafka(self):
        df = self.spark \
            .readStream \
            .format("kafka") \
            .options(**kafka_configs, **self.extra_kafka_configs) \
            .load()

        df = df.selectExpr("CAST(value AS STRING)")

        df = df.withColumn("value", from_json("value", self.schema)) \
            .select("value.*")

        return df

    def write_to_es(self, df, epoch_id):
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_configs, **self.extra_es_configs) \
            .mode("append") \
            .save()

    def process(self):
        df = self.read_from_kafka()

        # df = df.dropDuplicates(["user_name"])

        query = df \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(self.write_to_es) \
            .start()

        query.awaitTermination()

# if __name__ == "__main__":
#     kafka_to_es = MemberDataProcess()
#     kafka_to_es.process()