package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

/**
 * Etat de l'agregat depense. Le montant est cumule en micro-euros (long)
 * pour eviter les erreurs d'arrondi d'une somme de doubles.
 */
public record CampaignSpend(
        @JsonProperty("campaign_id") String campaignId,
        @JsonProperty("impressions") long impressions,
        @JsonProperty("spend_micro_eur") long spendMicroEur) {

    private static final long MICROS_PER_EUR = 1_000_000L;

    public static CampaignSpend empty() {
        return new CampaignSpend(null, 0, 0);
    }

    public CampaignSpend add(String campaignId, Impression impression) {
        return new CampaignSpend(campaignId, impressions + 1,
                spendMicroEur + Math.round(impression.paidPriceEur() * MICROS_PER_EUR));
    }

    public SpendReport toReport() {
        return new SpendReport(campaignId, impressions, BigDecimal.valueOf(spendMicroEur, 6));
    }
}
