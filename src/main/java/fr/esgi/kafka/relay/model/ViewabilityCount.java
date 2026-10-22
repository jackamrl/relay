package fr.esgi.kafka.relay.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Etat de l'agregat viewability : deux compteurs dans un seul aggregate. */
public record ViewabilityCount(
        @JsonProperty("viewable") long viewable,
        @JsonProperty("total") long total) {

    public static ViewabilityCount empty() {
        return new ViewabilityCount(0, 0);
    }

    public ViewabilityCount add(boolean isViewable) {
        return new ViewabilityCount(viewable + (isViewable ? 1 : 0), total + 1);
    }

    public double ratio() {
        return total == 0 ? 0.0 : (double) viewable / total;
    }
}
