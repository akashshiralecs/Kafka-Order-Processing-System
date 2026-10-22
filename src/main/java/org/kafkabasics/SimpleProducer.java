package org.kafkabasics;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SimpleProducer {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // wait for all in-sync replicas
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // no duplicates on retry

        ObjectMapper mapper = new ObjectMapper();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<ProducerRecord<String, String>> recordList = new ArrayList<>();
            Order o1 = new Order(1, "Laptop", 999.9f);

            recordList.add(new ProducerRecord<String, String>("orders", "order-101", mapper.writeValueAsString(o1)));
            // recordList.add(new ProducerRecord<String, String>("orders", "order-68",
            // "mouse21"));
            // recordList.add(new ProducerRecord<String, String>("orders", "order-69",
            // "mouse22"));
            // recordList.add(new ProducerRecord<String, String>("orders", "order-70",
            // "mouse23"));

            for (ProducerRecord<String, String> record : recordList) {
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        ex.printStackTrace();
                    } else {
                        System.out.println("Send message to topic: " + metadata.topic() + " with partition: "
                                + metadata.partition() + " metadata offset: " + metadata.offset());
                    }
                });
            }
            producer.flush();
        } catch (JsonProcessingException jse) {
            System.out.println("Error : " + jse);
        }

    }
}

class Order {
    int orderid;
    String item;
    float price;

    Order(int orderid, String item, float price) {
        this.orderid = orderid;
        this.item = item;
        this.price = price;
    }

    // Jackson needs a no-arg constructor for deserialization
    public Order() {
    }

    // getters & setters (Jackson uses these)
    public int getOrderId() {
        return orderid;
    }

    public void setOrderId(int orderId) {
        this.orderid = orderId;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

}