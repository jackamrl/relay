package fr.esgi.kafka.relay;

import fr.esgi.kafka.relay.common.JsonSerdes;
import fr.esgi.kafka.relay.model.Click;
import fr.esgi.kafka.relay.model.DlqRecord;
import fr.esgi.kafka.relay.model.Impression;
import fr.esgi.kafka.relay.validation.EventValidator;
import fr.esgi.kafka.relay.validation.Validated;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Construisez ici la topologie du projet RELAY.
 * Quarkus detecte ce @Produces Topology et demarre Kafka Streams
 * automatiquement (extension quarkus-kafka-streams).
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 */
@ApplicationScoped
public class TopologyProducer {

    /** Une banniere sous ce taux de visibilite rend le site suspect. */
    public static final double VIEWABILITY_MIN_RATIO = 0.40;
    /** En dessous de ce volume, le taux n'est pas significatif. */
    public static final int VIEWABILITY_MIN_IMPRESSIONS = 50;
    /** Un humain ne clique pas en moins d'une seconde. */
    public static final long BOT_MAX_DELAY_MS = 1000;
    /** Nombre de clics rapides a partir duquel on alerte. */
    public static final int BOT_MIN_CLICKS = 10;

    /**
     * Taille de la fenetre anti-robot, en minutes. Doit rester
     * configurable sans recompiler : l'enseignant la fera varier en
     * soutenance (BOT_WINDOW_MINUTES=5 par exemple).
     */
    @ConfigProperty(name = "BOT_WINDOW_MINUTES", defaultValue = "10")
    int botWindowMinutes;

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rawImpressions = builder.stream(
                Topics.IMPRESSIONS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawClicks = builder.stream(
                Topics.CLICKS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // RLY-1 - Ingestion fiable : seuls ces deux flux valides alimentent
        // les tickets suivants, tout le reste part en DLQ.
        KStream<String, Impression> impressions = validateOrDlq(
                rawImpressions, Topics.IMPRESSIONS, "impressions", EventValidator::impression);
        KStream<String, Click> clicks = validateOrDlq(
                rawClicks, Topics.CLICKS, "clicks", EventValidator::click);

        // RLY-2 - Viewability par site (tumbling 10 min)  -> Topics.VIEWABILITY
        //         Ratio viewable / total par site_id. Suspect si le ratio
        //         est sous VIEWABILITY_MIN_RATIO ET que la cellule compte
        //         au moins VIEWABILITY_MIN_IMPRESSIONS impressions.

        // RLY-3 - Top campagnes par pays (hopping 15/5 min,
        //         jointure GlobalKTable relay.campaigns) -> Topics.TOP_BY_GEO
        //         Certaines campagnes servies sont ABSENTES du catalogue :
        //         elles doivent survivre a la jointure.

        // RLY-4 - Depense par campagne                    -> Topics.SPEND
        //         Cumul de paid_price_eur par campaign_id (KTable,
        //         aggregate). Une impression n'est facturee qu'UNE fois.

        // RLY-5 - Tunnel impression -> clic, clics robots -> Topics.ALERTS_CLICKBOT
        //         Jointure flux-flux sur impression_id (les deux topics
        //         sont clees par user_id : il faut re-clef). Un clic a
        //         moins de BOT_MAX_DELAY_MS de son impression est un clic
        //         robot ; alerter au-dela de BOT_MIN_CLICKS clics robots
        //         par user sur une fenetre de botWindowMinutes.

        // RLY-6 (bonus) - Tests unitaires TopologyTestDriver (cf. README)

        return builder.build();
    }

    private static <T> KStream<String, T> validateOrDlq(KStream<String, String> raw,
                                                        String sourceTopic,
                                                        String name,
                                                        ValueMapper<String, Validated<T>> validator) {
        Map<String, KStream<String, Validated<T>>> branches = raw
                .mapValues(validator, Named.as(name + "-validation"))
                .split(Named.as(name + "-"))
                .branch((key, result) -> result.isValid(), Branched.as("valid"))
                .defaultBranch(Branched.as("invalid"));

        branches.get(name + "-invalid")
                .mapValues((key, result) -> JsonSerdes.toJson(new DlqRecord(
                        sourceTopic, key, result.reason(), result.detail(), result.raw())),
                        Named.as(name + "-to-dlq"))
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        return branches.get(name + "-valid").mapValues(Validated::value);
    }
}
