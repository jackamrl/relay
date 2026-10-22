package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Message publie dans <grp>.relay.viewability (cle = site_id). */
public record SiteViewability(
        @JsonProperty("site_id") String siteId,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd,
        @JsonProperty("viewable") long viewable,
        @JsonProperty("total") long total,
        @JsonProperty("viewability_ratio") double viewabilityRatio,
        @JsonProperty("significant") boolean significant,
        @JsonProperty("suspect") boolean suspect) {
}
