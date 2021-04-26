package com.github.kafka_trainnig;

import java.util.Properties;

import com.google.gson.JsonParser;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class KafkaFilterTweets {
    public static void main( String[] args )
    {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
       
        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // build topology
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
            (k, jsonTweets) -> extractIdFromTweets(jsonTweets) > 10000
        );
        filteredStream.to("important_tweets");
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();

        // start our stream app
    }

    private static Integer extractIdFromTweets(String message)
    { 
        try {
            return JsonParser.parseString(message)
            .getAsJsonObject()
            .get("user")
            .getAsJsonObject()
            .get("followers_count")
            .getAsInt();
        } catch (NullPointerException e){
            return 0;
        }
    }
}
