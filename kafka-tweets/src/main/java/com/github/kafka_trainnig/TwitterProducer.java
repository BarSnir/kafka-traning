package com.github.kafka_trainnig;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    List<String> terms = Lists.newArrayList("python","java","javascript","elasticsearch", "kafka");

    public TwitterProducer(){}

    public static void main(String[] args)
    {
        new TwitterProducer().run();
    }

    public void run()
    {
        //create a twitter producer
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        //create kafka producer
        KafkaProducer <String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping app.");
            client.stop();
            kafkaProducer.close();
        }));

        //loop to send tweets
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if( msg != null ) {
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg),new Callback(){
                    public void onCompletion(RecordMetadata recordMetadata, Exception e)
                    {
                        if(e != null){
                            logger.error("Something went wrong");
                        }
                    }
                });

            }
          }
    }

    String consumerKey = "VGSJ4vV2zwf65pu0ChLOnLbX9";
    String consumerSecret = "bKnZEU5zDvfCVxnBbaDu7tax5P03iawwQsSIQ1Qa7M7iQSvhiW";
    String token = "1190219192915681281-m849KlZ4VxDsZauqGI0ngicpqIO2jr";
    String secret = "RSophA41bJoFObk4xOulwJjmNHYoYSD1Gr9xn6Lu8N4Zb";

    public Client createTwitterClient( BlockingQueue<String> msgQueue)
    {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01")                              // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        Client client = builder.build();

        return client;
    }

    public KafkaProducer<String, String> createKafkaProducer()
    {
        String bootstrapServer = "localhost:9092";

        Properties Properties = new Properties();
        Properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        Properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //safe producer configuration
        Properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        Properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        Properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        Properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput producer (at expense a bit f latency & cpu usage)
        Properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        Properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        Properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));


        return new KafkaProducer<String, String>(Properties);
    }
}
