package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import fr.esgi.kafka.relay.validation.RejectReason;

/** Message publie dans <grp>.relay.dlq : le message original et la raison du rejet. */
public record DlqRecord(
        @JsonProperty("source_topic") String sourceTopic,
        @JsonProperty("key") String key,
        @JsonProperty("reason") RejectReason reason,
        @JsonProperty("detail") String detail,
        @JsonProperty("raw") String raw) {
}
