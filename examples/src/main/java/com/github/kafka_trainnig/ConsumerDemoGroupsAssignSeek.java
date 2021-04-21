package com.github.kafka_trainnig;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerDemoGroupsAssignSeek {
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args)
    {
        String bootstrapServer = "localhost:9092";
        String groupId = "seven-app";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<String, String>(properties);

        // Assign & Seek
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetsToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));
        consumer.seek(partitionToReadFrom, offsetsToReadFrom);


        int numOfMsgToRead = 5;
        int numOfMsg = 0;
        boolean keepRead = true;
        
        // poll new data
        while (keepRead){
            ConsumerRecords<String, String> records =consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String>record : records){
                logger.info(
                    "Key:"+ record.key() + "\n" + 
                    "value:" + record.value() + "\n" + 
                    "Partition" + record.partition() + "\n" + 
                    "Offset" + record.offset() + "\n" 
                );
                numOfMsg += 1;
                if(numOfMsg > numOfMsgToRead){
                    keepRead = false;
                    logger.info("Exiting consumer!");
                    break;
                }

            }
        }


    }
}
