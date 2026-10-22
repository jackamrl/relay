package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

/** Message publie dans <grp>.relay.spend (cle = campaign_id). */
public record SpendReport(
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("impressions") long impressions,
        @JsonProperty("spend_eur") BigDecimal spendEur) {
}
