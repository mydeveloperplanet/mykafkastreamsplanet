package com.mykafkaprocessingplanet;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Properties;

@RestController
public class KafkaProcessingController {

    private KafkaStreams streams1;
    private KafkaStreams streams2;

    @RequestMapping("/startProcessing1/")
    public void startProcessing1() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream("my-kafka-streams-topic").filter((key, value) -> ((String) value).endsWith("#latin")).to("my-kafka-streams-out1");

        final Topology topology = builder.build();

        streams1 = new KafkaStreams(topology, props);

        streams1.start();

    }

    @RequestMapping("/startProcessing2/")
    public void startProcessing2() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<Object, String> source = builder.stream("my-kafka-streams-topic");
        source.flatMapValues((ValueMapper<String, Iterable<String>>) value -> Arrays.asList(value.substring(value.indexOf("#"))))
                .groupBy((key, value) -> value)
                .count()
                .toStream()
                .to("my-kafka-streams-hashtagcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        streams2 = new KafkaStreams(topology, props);
        streams2.start();

    }

    @RequestMapping("/stop/")
    public void stopStreams() {
        streams1.close();
        streams2.close();
    }

}
