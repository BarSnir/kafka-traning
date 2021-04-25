package com.github.kafka_trainnig;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import com.google.gson.JsonParser;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public class KafkaElasticsearchConsumer 
{
    public static void main( String[] args ) throws IOException
    {
        Logger logger =  LoggerFactory.getLogger(KafkaElasticsearchConsumer.class.getClass());
        RestHighLevelClient client = createClient();
        String topic = "twitter_tweets";
        KafkaConsumer<String, String> consumer = createConsumer(topic);

        while (true){
            ConsumerRecords<String, String> records =consumer.poll(Duration.ofMillis(100));
            logger.info("Received "+records.count()+" items");
            BulkRequest bulk = new BulkRequest();

            for(ConsumerRecord<String, String>record : records){
                try {
                    String jsonString = record.value();
                    String id = extractIdFromTweets(record.value());
                    IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        null,
                        id
                    ).source(jsonString, XContentType.JSON);
                    bulk.add(indexRequest);
                    Thread.sleep(1000);
                } catch (InterruptedException e){
                   logger.warn("Bad dataset: "+record.value());
                }
            }
            if (records.count() > 0){
                client.bulk(bulk, RequestOptions.DEFAULT);
                logger.info("Committing offsets");
                consumer.commitSync();
                logger.info("Offset been committed!");
            }
        }
        //client.close();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServer = "localhost:9092";
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static RestHighLevelClient createClient()
    {
        RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
        return client;
    }

    private static String extractIdFromTweets(String message)
    { 
        return JsonParser.parseString(message)
                    .getAsJsonObject()
                    .get("id_str")
                    .getAsString();
    }
}
