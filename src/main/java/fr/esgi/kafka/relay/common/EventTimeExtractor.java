package fr.esgi.kafka.relay.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Temps evenement : le champ "timestamp" du JSON, et non l'heure d'envoi
 * vers Kafka. Sans lui, un evenement en retard de 2 h tomberait dans la
 * fenetre courante.
 */
public class EventTimeExtractor implements TimestampExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof String raw) {
            try {
                JsonNode ts = MAPPER.readTree(raw).get("timestamp");
                if (ts != null && ts.isTextual()) {
                    long millis = Instant.parse(ts.textValue()).toEpochMilli();
                    if (millis >= 0) {
                        return millis;
                    }
                }
            } catch (Exception ignored) {
                // Message illisible : il doit quand meme atteindre la DLQ,
                // on lui laisse donc l'horodatage Kafka.
            }
        }
        return record.timestamp();
    }
}
