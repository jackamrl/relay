package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Message publie dans <grp>.relay.alerts.clickbot (cle = user_id). */
public record ClickBotAlert(
        @JsonProperty("user_id") String userId,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd,
        @JsonProperty("window_minutes") int windowMinutes,
        @JsonProperty("bot_clicks") long botClicks) {
}
