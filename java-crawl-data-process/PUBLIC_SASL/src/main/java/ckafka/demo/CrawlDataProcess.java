package ckafka.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class CrawlDataProcess {
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) throws Exception {
        // 创建配置对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkStreaming");

        // 创建环境对象
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2 * 1000));

        // 使用kafka作为数据源
        // 创建配置参数
        // 加载kafka.properties。
        Properties kafkaProperties = KafkaConfigure.getCKafkaProperties();
        //设置 jaas 配置信息
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, kafkaProperties.getProperty("java.security.auth.login.config"));
        Map<String, Object> kafkaParams = new HashMap<>();
        //
        // SASL_PLAINTEXT 公网接入
        //
        kafkaParams.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        // SASL 采用 Plain 方式。
        kafkaParams.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        // 设置接入点，请通过控制台获取对应Topic的接入点。
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        // 消息的反序列化方式。
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 当前消费实例所属的消费组，请在控制台申请之后填写。
        // 属于同一个组的消费实例，会负载消费消息。
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getProperty("group.id"));

        // 需要消费的主题
        List<String> topics = new ArrayList<>();
        topics.add(kafkaProperties.getProperty("topic"));

        JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferBrokers(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        // 创建 KafkaProducer 对象
        createKafkaProducer(kafkaProperties);

        directStream.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                String value = record.value();
                String event = getEventFromJson(value);
                System.out.println(value);
                // 根据事件类型发送到不同的主题
                switch (event) {
                    case "Member Message":
                        sendToKafkaTopic(value, "member_topic");
                        break;
                    case "Gift Message":
                        sendToKafkaTopic(value, "gift_topic");
                        break;
                    case "Chat Message":
                        sendToKafkaTopic(value, "chat_topic");
                        break;
                    case "Like Message":
                        sendToKafkaTopic(value, "like_topic");
                        break;
                    case "Social Message":
                        sendToKafkaTopic(value, "social_topic");
                        break;
                    default:
                        System.out.println("Unknown event type: " + event);
                }
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }

    // 创建 KafkaProducer 对象
    private static void createKafkaProducer(Properties kafkaProperties) {
        Properties props = new Properties();
        // 设置安全协议
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        // 设置 SASL 机制
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        // 设置接入点
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        // 设置序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 创建 Kafka 生产者对象
        producer = new KafkaProducer<>(props);
    }

    // 从 JSON 数据中获取事件类型
    private static String getEventFromJson(String json) {
        try {
            // 创建 ObjectMapper 对象
            ObjectMapper mapper = new ObjectMapper();
            // 解析 JSON 字符串为 JsonNode
            JsonNode rootNode = mapper.readTree(json);
            // 获取事件类型
            String event = rootNode.get("event").asText();
            // 返回事件类型
            return event;
        } catch (Exception e) {
            e.printStackTrace();
            return ""; // 解析失败时返回空字符串
        }
    }

    // 发送数据到指定的 Kafka 主题
    private static void sendToKafkaTopic(String value, String topic) {
        // 构造消息对象
        ProducerRecord<String, String> kafkaMessage = new ProducerRecord<>(topic, value);

        try {
            // 发送消息，并获得一个 Future 对象
            Future<RecordMetadata> metadataFuture = producer.send(kafkaMessage);

            // 同步获得 Future 对象的结果
            RecordMetadata recordMetadata = metadataFuture.get();
            System.out.println("Produce ok:" + recordMetadata.toString());
        } catch (Exception e) {
            // 处理异常
            System.out.println("Error occurred while sending message to Kafka topic: " + e.getMessage());
        }
    }
}
