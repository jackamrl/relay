package fr.esgi.kafka.relay.processing;

import fr.esgi.kafka.relay.model.Impression;
import java.time.Duration;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * Ne laisse passer qu'une fois chaque impression_id. Les identifiants vus
 * sont gardes dans un store fenetre, purge automatiquement a l'expiration
 * de sa retention. Le flux doit etre cle par impression_id, pour que toutes
 * les copies d'une impression arrivent sur la meme tache.
 */
public class ImpressionDeduplicator implements FixedKeyProcessor<String, Impression, Impression> {

    private final String storeName;
    private final long windowMs;
    private FixedKeyProcessorContext<String, Impression> context;
    private WindowStore<String, Long> seen;

    public ImpressionDeduplicator(String storeName, Duration window) {
        this.storeName = storeName;
        this.windowMs = window.toMillis();
    }

    @Override
    public void init(FixedKeyProcessorContext<String, Impression> context) {
        this.context = context;
        this.seen = context.getStateStore(storeName);
    }

    @Override
    public void process(FixedKeyRecord<String, Impression> record) {
        String impressionId = record.value().impressionId();
        long ts = record.timestamp();
        try (WindowStoreIterator<Long> previous =
                     seen.fetch(impressionId, Math.max(0, ts - windowMs), ts + windowMs)) {
            if (previous.hasNext()) {
                return;
            }
        }
        seen.put(impressionId, ts, ts);
        context.forward(record);
    }
}
