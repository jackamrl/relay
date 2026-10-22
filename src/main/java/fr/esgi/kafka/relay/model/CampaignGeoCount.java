package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Etat de l'agregat top par pays : le compteur et la fiche campagne. */
public record CampaignGeoCount(
        @JsonProperty("geo") String geo,
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("name") String name,
        @JsonProperty("advertiser") String advertiser,
        @JsonProperty("in_catalog") boolean inCatalog,
        @JsonProperty("impressions") long impressions) {

    public static CampaignGeoCount empty() {
        return new CampaignGeoCount(null, null, null, null, false, 0);
    }

    public CampaignGeoCount add(CampaignImpression impression) {
        return new CampaignGeoCount(impression.geo(), impression.campaignId(), impression.name(),
                impression.advertiser(), impression.inCatalog(), impressions + 1);
    }
}
