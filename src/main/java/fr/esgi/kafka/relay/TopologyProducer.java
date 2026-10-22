package fr.esgi.kafka.relay;

import fr.esgi.kafka.relay.common.EventTimeExtractor;
import fr.esgi.kafka.relay.common.JsonSerdes;
import fr.esgi.kafka.relay.model.Click;
import fr.esgi.kafka.relay.model.DlqRecord;
import fr.esgi.kafka.relay.model.Impression;
import fr.esgi.kafka.relay.model.SiteViewability;
import fr.esgi.kafka.relay.model.ViewabilityCount;
import fr.esgi.kafka.relay.validation.EventValidator;
import fr.esgi.kafka.relay.validation.Validated;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
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
    public static final Duration VIEWABILITY_WINDOW = Duration.ofMinutes(10);
    /** Les retardataires arrivent jusqu'a 180 min apres leur horodatage. */
    public static final Duration LATE_EVENTS_GRACE = Duration.ofMinutes(180);
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
                Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new EventTimeExtractor()));

        KStream<String, String> rawClicks = builder.stream(
                Topics.CLICKS,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new EventTimeExtractor()));

        // RLY-1 - Ingestion fiable : seuls ces deux flux valides alimentent
        // les tickets suivants, tout le reste part en DLQ.
        KStream<String, Impression> impressions = validateOrDlq(
                rawImpressions, Topics.IMPRESSIONS, "impressions", EventValidator::impression);
        KStream<String, Click> clicks = validateOrDlq(
                rawClicks, Topics.CLICKS, "clicks", EventValidator::click);

        // RLY-2 - Viewability par site (tumbling 10 min)  -> Topics.VIEWABILITY
        buildViewability(impressions);

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

    private static void buildViewability(KStream<String, Impression> impressions) {
        impressions
                .map((userId, imp) -> KeyValue.pair(imp.siteId(), imp.viewable()),
                        Named.as("impressions-by-site"))
                .groupByKey(Grouped.with("viewability-by-site", Serdes.String(), Serdes.Boolean()))
                .windowedBy(TimeWindows.ofSizeAndGrace(VIEWABILITY_WINDOW, LATE_EVENTS_GRACE))
                .aggregate(ViewabilityCount::empty,
                        (siteId, viewable, count) -> count.add(viewable),
                        Named.as("viewability-count"),
                        Materialized.<String, ViewabilityCount, WindowStore<Bytes, byte[]>>as("viewability-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.of(ViewabilityCount.class)))
                .toStream(Named.as("viewability-updates"))
                .map((window, count) -> KeyValue.pair(window.key(), JsonSerdes.toJson(toViewability(window, count))),
                        Named.as("viewability-to-json"))
                .to(Topics.VIEWABILITY, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static SiteViewability toViewability(Windowed<String> window, ViewabilityCount count) {
        boolean significant = count.total() >= VIEWABILITY_MIN_IMPRESSIONS;
        return new SiteViewability(
                window.key(),
                window.window().startTime().toString(),
                window.window().endTime().toString(),
                count.viewable(),
                count.total(),
                count.ratio(),
                significant,
                significant && count.ratio() < VIEWABILITY_MIN_RATIO);
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
