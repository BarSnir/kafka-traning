package com.github.kafka_trainnig;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ProducerDemo {
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args)
    {
        String bootstrapServer = "localhost:9092";

        Properties Properties = new Properties();
        // Old-Way
        // Properties.setProperty("bootstrap.server", bootstrapServer);
        // Properties.setProperty("key.serializer", StringSerializer.class.getName());
        // Properties.setProperty("value.serializer", StringSerializer.class.getName());

        //New-Way
        Properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        Properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<String, String>(Properties);

        //Create Producer Record
        ProducerRecord <String, String> record = new ProducerRecord<String, String>("first_topic", "Hello world.");

        //Send data - async
        producer.send(record);

        // Wait's to sending
        producer.flush();
        producer.close();

        System.out.println( "Hello Producer!" );
    }
}
