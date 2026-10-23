package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * Paire impression-clic apres la jointure flux-flux.
 * delta_ms = heure du clic - heure de l'impression. Negatif si le clic
 * est un retardataire : ce n'est pas un robot.
 */
public record ClickMatch(
        @JsonProperty("user_id") String userId,
        @JsonProperty("click_id") String clickId,
        @JsonProperty("impression_id") String impressionId,
        @JsonProperty("delta_ms") long deltaMs) {

    public static ClickMatch from(Impression impression, Click click) {
        long delta = Instant.parse(click.timestamp()).toEpochMilli()
                - Instant.parse(impression.timestamp()).toEpochMilli();
        return new ClickMatch(click.userId(), click.clickId(), click.impressionId(), delta);
    }

    /** Un humain ne clique pas en moins de maxDelayMs, et un retard n'est pas un robot. */
    public boolean isBot(long maxDelayMs) {
        return deltaMs >= 0 && deltaMs < maxDelayMs;
    }
}
