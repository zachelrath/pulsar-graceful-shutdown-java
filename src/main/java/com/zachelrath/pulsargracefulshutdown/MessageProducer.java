package com.zachelrath.pulsargracefulshutdown;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

@Configuration
@ConditionalOnProperty(name = "producer.enabled", havingValue = "true")
@Slf4j
public class MessageProducer {

    private final PulsarClient client;

    private final Producer<byte[]> producer;

    private final Long sendIntervalMillis;

    private static final List<String> keys = Arrays.asList("comfrey", "echinacea", "verbena", "sorrel");

    public MessageProducer(
            @Value("${pulsar.url}") String pulsarUrl,
            @Value("${pulsar.topic.name}") String topicName,
            @Value("${producer.send.interval.millis}") Long sendIntervalMillis
    ) throws PulsarClientException {
        this.sendIntervalMillis = sendIntervalMillis;
        client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build();
        log.info("(PRODUCER) Connected to Pulsar client");

        producer = client.newProducer()
                .producerName("main-producer")
                .topic(topicName)
                .create();
        log.info("(PRODUCER) Created producer");
    }

    public void send(String key, String message) throws PulsarClientException {
        MessageId msgId = producer.newMessage()
                .property("data", message)
                .key(key)
                .send();
        log.info("(PRODUCER) Sent [{}]: {}", key, msgId);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        // Producer messages at regular cadences
        while (true) {
            // Produce messages at regular interval
            try {
                Thread.sleep(sendIntervalMillis);
            } catch (InterruptedException e) {
                log.error("(PRODUCER) THREAD interrupted!!");
                e.printStackTrace();
            }
            try {
                send(getRandomKey(), Instant.now().toString());
            } catch (PulsarClientException e) {
                log.error("(PRODUCER) Error producing message to pulsar", e);
            }
        }
    }

    private String getRandomKey() {
        return keys.get((int) (Math.ceil(Math.random() * keys.size()) - 1));
    }

}
