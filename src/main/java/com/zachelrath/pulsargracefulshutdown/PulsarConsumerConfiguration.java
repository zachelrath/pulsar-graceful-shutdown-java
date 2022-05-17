package com.zachelrath.pulsargracefulshutdown;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Configuration
@ConditionalOnProperty(name = "consumer.enabled", havingValue = "true")
@Slf4j
public class PulsarConsumerConfiguration {

    // This property should
    private final int shutdownGracePeriodSeconds;
    private final PulsarClient client;

    private final List<Consumer> consumers;
    private final SimpleMessageListener msgListener;

    public PulsarConsumerConfiguration(
            @Value("${pulsar.url}") String pulsarUrl,
            @Value("${pulsar.topic.name}") String topicName,
            @Value("${pulsar.subscription.name}") String subscriptionName,
            @Value("${consumer.shutdown.grace.seconds}") int shutdownGracePeriodSeconds,
            SimpleMessageListener messageListener
    ) throws PulsarClientException {
        this.msgListener = messageListener;
        this.shutdownGracePeriodSeconds = shutdownGracePeriodSeconds;
        client = PulsarClient.builder()
                .serviceUrl(pulsarUrl)
                .build();
        log.info("(CONSUMER) Connected to Pulsar client");
        consumers = new ArrayList<>();
        Consumer<byte[]> consumer = client.newConsumer()
                .subscriptionName(subscriptionName)
                .topic(topicName)
                .receiverQueueSize(5)
                .messageListener(messageListener)
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscribe();

        consumers.add(consumer);

        log.info("(CONSUMER) Subscribed to topic {}", topicName);
    }

    @EventListener(ContextClosedEvent.class)
    public void onContextClosed() {
        log.info("(CONSUMER) CONTEXT CLOSED received, initiating graceful shutdown...");
        // Stop accepting new messages from Broker by pausing all consumers
        log.info("Pausing all consumers...");
        consumers.forEach(Consumer::pause);

        // Instruct the message listener to enter graceful shutdown mode.
        // The details of this may differ based on your application, and depending on the receiver queue size,
        // but if there are messages still in the receiver queue, we have 2 choices on how to handle them:
        // (A) Negative acknowledge the messages immediately, to force the broker to reprocess them later
        // (B) Allow the consumer to finish processing all messages in its receiver queue.
        // If consumers can be expected to finish processing their receiver queue before the application is killed,
        // then (B) is fine, but if the receiver queue size is large or the shutdown grace period is short,
        // then this is risky and (A) should be used instead.
        log.info("Initiating graceful shutdown on the message listener (to negative ack all messages yet to be processed in the receiver queue)...");
        msgListener.initiateGracefulShutdown();
        // Wait for a time to allow messages to finish being processed
        try {
            TimeUnit.SECONDS.sleep(shutdownGracePeriodSeconds);
        } catch (InterruptedException e) {
            log.error("THREAD INTERRUPTED");
        }
        log.info("Closing all consumers...");
        consumers.forEach(consumer -> {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                log.error("UNABLE TO close consumer", e);
            }
        });
        log.info("Successfully closed consumers!");
        log.info("Closing client...");
        try {
            client.close();
        } catch (PulsarClientException e) {
            log.error("UNABLE to close client...", e);
        }

        log.info("Graceful shutdown complete!");
    }

}
