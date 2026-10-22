package fr.esgi.kafka.relay;

import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Noms des topics du projet.
 * Entrees fournies par l'enseignant (lecture seule) ; sorties prefixees
 * par votre code groupe (variable d'environnement GROUPE, ex: grp07).
 */
public final class Topics {

    private Topics() {
    }

    // --- Entrees (fournies, ne pas ecrire dedans) ---
    public static final String IMPRESSIONS = "relay.impressions";
    public static final String CLICKS = "relay.clicks";
    public static final String CAMPAIGNS = "relay.campaigns";

    /** Code groupe, obligatoire sur le cluster partage. */
    public static final String GROUPE = ConfigProvider.getConfig()
            .getOptionalValue("GROUPE", String.class)
            .orElse("grp00");

    public static String out(String suffix) {
        return GROUPE + "." + suffix;
    }

    // --- Sorties (a produire par votre application) ---
    public static final String DLQ = out("relay.dlq");
    public static final String VIEWABILITY = out("relay.viewability");
    public static final String TOP_BY_GEO = out("relay.top.by-geo");
    public static final String SPEND = out("relay.spend");
    public static final String ALERTS_CLICKBOT = out("relay.alerts.clickbot");
}
