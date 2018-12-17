package com.github.churtado.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {
    static Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

    public static void main(String[] args) {


        // create properties

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_bitcoin");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) ->  extractUserFollowersInTweet(jsonTweet) > 10000
                // filter for tweets which has user of over 10000 followers
        );

        filteredStream.to("important_tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start stream application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowersInTweet(String tweetJson){
        logger.info("entering method");
        // gson library
        try{
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("User")
                    .getAsJsonObject()
                    .get("FollowersCount")
                    .getAsInt();
        }catch(NullPointerException e){
            logger.info("tried to get something but got error", e);
            return 0;
        }

    }
}
