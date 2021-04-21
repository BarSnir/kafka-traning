package com.github.kafka_trainnig;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;

public class ProducerKeysDemo {
    private static KafkaProducer<String, String> producer;
    private static Logger logger;

    public static void main(String[] args)
    {
        String bootstrapServer = "localhost:9092";
        logger = LoggerFactory.getLogger(ProducerCBDemo.class);

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
        for(int i=0;i < 10; i++ ){
            ProducerRecord <String, String> record = new ProducerRecord<String, String>("first_topic", "Message V2 No."+ (i+1) );
            //Send data - async
            producer.send(record, new Callback(){
                public void onCompletion(RecordMetadata recordMetadata, Exception e) 
                {
                    //execute when record is sent or exception has raised.
                    if (e == null){
                        System.out.print("Received new meta data \n"+
                        "Topic:" + recordMetadata.topic() + "\n" + 
                        "Partition:" + recordMetadata.partition() + "\n" + 
                        "Offset:" + recordMetadata.offset() + "\n" + 
                        "Timestamp:" + recordMetadata.timestamp() + "\n" + "\n"
                        );
    
                    } else {
                        logger.error("Error while producing.", e);
                    }
                }
            });
        }
        

        // Wait's to sending
        producer.flush();
        producer.close();

        System.out.println( "Hello Producer!" );
    }
}
