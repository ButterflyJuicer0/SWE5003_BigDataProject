# import findspark
# findspark.init()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, expr
# from pyspark.sql.types import StructType, StringType
# from config.kafka_config import kafka_configs
# from config.es_config import es_configs
# from pyspark.sql.types import StringType
# from transformers import DistilBertForSequenceClassification,DistilBertTokenizer
# import torch
# from torch.nn.functional import softmax
#
# # 创建 SparkSession 对象
# spark = SparkSession.builder \
#     .appName("ReadFromKafka") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3") \
#     .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
#     .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
#     .getOrCreate()
#     # 定义 Kafka 配置
# extra_kafka_configs = {
#     'subscribe': 'chat_topic',
#     'kafka.group.id': 'chat_consumer',
# }
#
# extra_es_configs = {
#     'es.resource': 'chat_data/_doc'
# }
#
# # 定义数据模式
# schema = StructType().add("timestamp", StringType()) \
#     .add("event", StringType()) \
#     .add("user_name", StringType()) \
#     .add("content", StringType()) \
#
# # 定义 Spark UDF
# sentiment={0:'positive',1:'neutral',2:'negative'}
# def sentiment_analysis(text):
#     # 将模型加载到 GPU 上
#     model = DistilBertForSequenceClassification.from_pretrained('./model').to('cuda')
#     # 将标记器加载到 CPU 上（因为它不需要在 GPU 上处理）
#     tokenizers = DistilBertTokenizer.from_pretrained('./model')
#     # 将输入数据转换为 PyTorch 张量并移动到 GPU 上
#     inputs = tokenizers(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
#     inputs = {key: value.to('cuda') for key, value in inputs.items()}  # 将输入数据移动到 GPU 上
#     with torch.no_grad():
#         # 在 GPU 上执行模型推断
#         outputs = model(**inputs)
#         # 在 GPU 上计算 softmax
#         probs = softmax(outputs.logits, dim=1)
#     # 将结果从 GPU 移动到 CPU 并进行后续处理
#     sentiment_type = torch.argmax(probs, dim=1).cpu().numpy()[0]
#     return sentiment[sentiment_type]
#
#
# # 注册 Spark UDF
# spark.udf.register("sentiment_analysis_udf", sentiment_analysis, StringType())
#
# # 从 Kafka 主题中读取数据
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .options(**kafka_configs, **extra_kafka_configs) \
#     .load()
#
# # 解码 Kafka 消息的值，并将其解析为 DataFrame
# df = df.selectExpr("CAST(value AS STRING)")
#
# # 将 JSON 字符串解析为 DataFrame
# df = df.withColumn("value", from_json("value", schema)) \
#     .select("value.*")
#
# # 定义写入 Elasticsearch 的函数
# def write_to_es(df, epoch_id):
#     processed_df = df.withColumn("sentiment", expr("sentiment_analysis_udf(content)"))
#     processed_df.write \
#         .format("org.elasticsearch.spark.sql") \
#         .options(**es_configs, **extra_es_configs) \
#         .mode("append") \
#         .save()
#
# # 将数据写入 Elasticsearch
# query = df \
#     .writeStream \
#     .outputMode("append") \
#     .foreachBatch(write_to_es) \
#     .start()
#
# query.awaitTermination()
#
#


# stream_processor.py

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, expr
from pyspark.sql.types import StructType, StringType
from transformers import DistilBertForSequenceClassification, DistilBertTokenizer
import torch
from torch.nn.functional import softmax
from config.kafka_config import kafka_configs
from config.es_config import es_configs

def chat_data_process():
    spark = SparkSession.builder \
        .appName("ReadFromKafka") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .getOrCreate()

    extra_kafka_configs = {
        'subscribe': 'chat_topic',
        'kafka.group.id': 'chat_consumer',
    }

    extra_es_configs = {
        'es.resource': 'chat_data/_doc'
    }

    schema = StructType().add("timestamp", StringType()) \
        .add("event", StringType()) \
        .add("user_name", StringType()) \
        .add("content", StringType())

    sentiment={0:'positive',1:'neutral',2:'negative'}
    def sentiment_analysis(text):
        model = DistilBertForSequenceClassification.from_pretrained('./model').to('cuda')
        tokenizers = DistilBertTokenizer.from_pretrained('./model')
        inputs = tokenizers(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
        inputs = {key: value.to('cuda') for key, value in inputs.items()}
        with torch.no_grad():
            outputs = model(**inputs)
            probs = softmax(outputs.logits, dim=1)
        sentiment_type = torch.argmax(probs, dim=1).cpu().numpy()[0]
        return sentiment[sentiment_type]

    spark.udf.register("sentiment_analysis_udf", sentiment_analysis, StringType())

    df = spark \
        .readStream \
        .format("kafka") \
        .options(**kafka_configs, **extra_kafka_configs) \
        .load()

    df = df.selectExpr("CAST(value AS STRING)")

    df = df.withColumn("value", from_json("value", schema)) \
        .select("value.*")

    def write_to_es(df, epoch_id):
        processed_df = df.withColumn("sentiment", expr("sentiment_analysis_udf(content)"))
        processed_df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_configs, **extra_es_configs) \
            .mode("append") \
            .save()

    query = df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_es) \
        .start()

    query.awaitTermination()
