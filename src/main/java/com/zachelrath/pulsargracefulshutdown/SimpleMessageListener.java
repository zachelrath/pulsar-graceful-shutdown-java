package com.zachelrath.pulsargracefulshutdown;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@ConditionalOnProperty(name = "consumer.enabled", havingValue = "true")
public class SimpleMessageListener implements MessageListener<byte[]> {

    private final int maxConsumeDelaySeconds;
    private boolean isAcceptingNewMessages;
    private static final String CONSUMER_PREFIX = "(CONSUMER) [{}]: ";

    public SimpleMessageListener(
            @Value("${consumer.max.consume.delay.seconds}") int maxConsumeDelaySeconds) {
        this.maxConsumeDelaySeconds = maxConsumeDelaySeconds;
        this.isAcceptingNewMessages = true;
    }

    public void initiateGracefulShutdown() {
        this.isAcceptingNewMessages = false;
    }

    @Override
    public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
        log.info(CONSUMER_PREFIX + "Received, beginning to process ...", msg.getMessageId());

        if (!isAcceptingNewMessages) {
            log.info(CONSUMER_PREFIX + ">>> NOT accepting new messages! Negative acking.", msg.getMessageId());
            consumer.negativeAcknowledge(msg);
            return;
        }

        try {
            // Sleep for a random delay to simulate delay in message consumption/processing
            TimeUnit.SECONDS.sleep((int) Math.ceil(Math.random() * maxConsumeDelaySeconds));
            consumer.acknowledge(msg);
            log.info(CONSUMER_PREFIX + "Successfully processed!", msg.getMessageId());
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.error("(CONSUMER) THREAD INTERRUPTED --- DID NOT FINISH PROCESSING!");
        } catch (PulsarClientException e) {
            log.error("(CONSUMER) Pulsar client exception", e);
            consumer.negativeAcknowledge(msg);
        }
    }
}
