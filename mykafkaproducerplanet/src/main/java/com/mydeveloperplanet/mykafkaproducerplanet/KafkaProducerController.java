package com.mydeveloperplanet.mykafkaproducerplanet;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.Random;

@RestController
public class KafkaProducerController {

    private static final String loremIpsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor " +
            "incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco " +
            "laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit " +
            "esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa " +
            "qui officia deserunt mollit anim id est laborum.";

    private static final String[] hashTags = {"latin", "italy", "roman", "caesar", "cicero"};

    private Random randomNumber = new Random();

    private String randomMessage;

    @RequestMapping("/sendMessages/")
    public void sendMessages() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            while (true) {
                // Every second send a message
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {}

                randomMessage = loremIpsum + " #" + hashTags[randomNumber.nextInt(hashTags.length)];
                producer.send(new ProducerRecord<String, String>("my-kafka-streams-topic", null, randomMessage));

            }
        } finally {
            producer.close();
        }

    }

}
