package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Affichage d'une banniere (topic relay.impressions, cle = user_id).
 * device : TV | MOBILE | WEB | CONSOLE.
 * geo : FR | US | DE | BR | JP | GB.
 * viewable : la banniere a-t-elle ete reellement visible (norme IAB).
 * paid_price_eur : prix paye a l'editeur pour CET affichage.
 */
public record Impression(
        @JsonProperty("impression_id") String impressionId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("creative_id") String creativeId,
        @JsonProperty("site_id") String siteId,
        @JsonProperty("device") String device,
        @JsonProperty("geo") String geo,
        @JsonProperty("viewable") Boolean viewable,
        @JsonProperty("paid_price_eur") Double paidPriceEur,
        @JsonProperty("timestamp") String timestamp) {
}
