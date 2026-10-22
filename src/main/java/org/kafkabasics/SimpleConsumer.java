package org.kafkabasics;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleConsumer {

    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Writing code for Kafka consumer");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processing");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // start from
        // beginning if no offset
        // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // commit
        // manually after processing

        int value = 1;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("orders"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                consumer.commitSync();
                for (ConsumerRecord cr : consumerRecords) {
                    System.out.println("Fectching key " + cr.key() + " value: " + cr.value() + " :: Offset: "
                            + cr.offset() + " :: partition: " + cr.partition() + " :: consumer-grp-name: "
                            + props.getProperty("group.id"));

                    if (value == 2) {
                        System.out.println("Sleeping for 10 secs...:value:: " + value);
                        Thread.sleep(10000);
                    }
                    value++;
                }

            }
        }

    }
}
