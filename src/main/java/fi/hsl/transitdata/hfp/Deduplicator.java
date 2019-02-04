package fi.hsl.transitdata.hfp;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;

public class Deduplicator implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(Deduplicator.class);

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;

    private final Cache<HashCode, Long> hashCache;
    private final HashFunction hashFunction = Hashing.murmur3_128();

    public Deduplicator(PulsarApplicationContext context) {
        consumer = context.getConsumer();
        producer = context.getProducer();

        Duration ttl = context.getConfig().getDuration("application.cacheTTL");
        hashCache = CacheBuilder.newBuilder()
                .expireAfterAccess(ttl)
                .initialCapacity(1024*1024)
                .build();
    }

    public void handleMessage(Message received) throws Exception {
        try {
            HashCode hash = hashFunction.hashBytes(received.getData());
            Long cacheHit = hashCache.getIfPresent(hash);
            if (cacheHit == null) {
                // We haven't yet received this so save to cache and send the message.
                // Timestamp is just for debugging, no other value
                hashCache.put(hash, System.currentTimeMillis());
                sendPulsarMessage(received);
            }
            else {
                long elapsedSinceHit = System.currentTimeMillis() - cacheHit;
            }
            ack(received.getMessageId());
        }
        catch (Exception e) {
            log.error("Exception while handling message", e);
        }
    }

    private void ack(MessageId received) {
        consumer.acknowledgeAsync(received)
                .exceptionally(throwable -> {
                    log.error("Failed to ack Pulsar message", throwable);
                    return null;
                })
                .thenRun(() -> {});
    }

    private void sendPulsarMessage(Message toSend) {

        producer.newMessage()
                //.key(dvjId) //TODO think about this
                .eventTime(toSend.getEventTime())
                .properties(toSend.getProperties())
                .value(toSend.getData())
                .sendAsync()
                .exceptionally(t -> {
                    log.error("Failed to send Pulsar message", t);
                    return null;
                }) .thenRun(() -> {});

    }
}
