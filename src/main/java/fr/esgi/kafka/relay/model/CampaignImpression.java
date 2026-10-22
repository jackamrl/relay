package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Impression enrichie par le catalogue, reduite a ce dont le comptage a
 * besoin : c'est ce qui transite dans le topic de re-partition.
 * name et advertiser sont null pour une campagne hors catalogue.
 */
public record CampaignImpression(
        @JsonProperty("geo") String geo,
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("name") String name,
        @JsonProperty("advertiser") String advertiser,
        @JsonProperty("in_catalog") boolean inCatalog) {

    public static CampaignImpression of(Impression impression, Campaign campaign) {
        return campaign == null
                ? new CampaignImpression(impression.geo(), impression.campaignId(), null, null, false)
                : new CampaignImpression(impression.geo(), impression.campaignId(),
                        campaign.name(), campaign.advertiser(), true);
    }
}
