package edu.supmti.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {

        // Configuration de Kafka Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop-master:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // message test
        String topic = "test-topic";
        String message = "Bonjour depuis Java Producer !";

        producer.send(new ProducerRecord<>(topic, message));
        System.out.println("Message envoyé : " + message);

        producer.close();
    }
}
