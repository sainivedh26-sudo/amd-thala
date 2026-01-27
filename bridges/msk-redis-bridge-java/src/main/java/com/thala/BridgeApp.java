package com.thala;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class BridgeApp {
    public static void main(String[] args) {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "");
        String groupId = env("KAFKA_GROUP_ID", "thala-redis-bridge");
        String topicsCsv = env("KAFKA_TOPICS", "thala-slack-events,thala-jira-events,thala-email-events");

        String redisHost = env("REDIS_HOST", "127.0.0.1");
        int redisPort = Integer.parseInt(env("REDIS_PORT", "6379"));
        String redisPassword = env("REDIS_PASSWORD", "");
        String redisListPrefix = env("REDIS_LIST_PREFIX", "thala:queue:");

        if (bootstrap.isEmpty()) {
            System.err.println("KAFKA_BOOTSTRAP_SERVERS is required");
            System.exit(1);
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // MSK IAM auth
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicsCsv.split(",")));

        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            if (!redisPassword.isEmpty()) {
                jedis.auth(redisPassword);
            }

            System.out.println("MSK→Redis bridge started. Subscribed to: " + topicsCsv);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String listKey = redisListPrefix + record.topic();
                    // Push JSON payload as is; Python will BLPOP from this list
                    jedis.rpush(listKey, record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return v != null ? v : d;
    }
}




