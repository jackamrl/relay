package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Message publie dans <grp>.relay.top.by-geo (cle = geo). */
public record GeoCampaignTop(
        @JsonProperty("geo") String geo,
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("name") String name,
        @JsonProperty("advertiser") String advertiser,
        @JsonProperty("in_catalog") boolean inCatalog,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd,
        @JsonProperty("impressions") long impressions) {
}
