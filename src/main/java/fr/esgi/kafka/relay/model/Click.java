package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Clic sur une banniere (topic relay.clicks, cle = user_id).
 * impression_id porte l'affichage dont decoule le clic : c'est la cle de
 * jointure du tunnel impression -> clic.
 */
public record Click(
        @JsonProperty("click_id") String clickId,
        @JsonProperty("impression_id") String impressionId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("device") String device,
        @JsonProperty("geo") String geo,
        @JsonProperty("timestamp") String timestamp) {
}
