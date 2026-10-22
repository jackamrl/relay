package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Fiche campagne (topic compacte relay.campaigns, cle = campaign_id). */
public record Campaign(
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("name") String name,
        @JsonProperty("advertiser") String advertiser,
        @JsonProperty("vertical") String vertical,
        @JsonProperty("daily_budget_eur") Double dailyBudgetEur,
        @JsonProperty("cpc_max_eur") Double cpcMaxEur) {
}
