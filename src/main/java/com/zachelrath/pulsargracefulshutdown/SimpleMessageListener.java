package com.zachelrath.pulsargracefulshutdown;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@ConditionalOnProperty(name = "consumer.enabled", havingValue = "true")
public class SimpleMessageListener implements MessageListener<byte[]> {

    private final Long maxConsumeDelayMillis;
    private boolean isAcceptingNewMessages;

    public SimpleMessageListener(
            @Value("${consumer.max.consume.delay.millis}") Long maxConsumeDelayMillis) {
        this.maxConsumeDelayMillis = maxConsumeDelayMillis;
        this.isAcceptingNewMessages = true;
    }

    public void initiateGracefulShutdown() {
        this.isAcceptingNewMessages = false;
    }

    @Override
    public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
        log.info("(CONSUMER) Received [{} {}], beginning to process...", msg.getKey(), msg.getMessageId());

        if (!isAcceptingNewMessages) {
            log.info("NOT accepting new messages! Negative acking msg [{} {}]", msg.getKey(), msg.getMessageId());
            consumer.negativeAcknowledge(msg);
            return;
        }

        try {
            // Sleep for a random delay to simulate delay in message consumption/processing
            Thread.sleep((long) Math.ceil(Math.random() * maxConsumeDelayMillis));
            consumer.acknowledge(msg);
            log.info("(CONSUMER) Successfully processed [{}, {}]", msg.getKey(), msg.getMessageId());
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.error("(CONSUMER) THREAD INTERRUPTED --- DID NOT FINISH PROCESSING!");
        } catch (PulsarClientException e) {
            log.error("(CONSUMER) Pulsar client exception", e);
            consumer.negativeAcknowledge(msg);
        }
    }
}
