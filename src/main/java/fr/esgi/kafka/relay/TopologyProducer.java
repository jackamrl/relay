package fr.esgi.kafka.relay;

import fr.esgi.kafka.relay.common.EventTimeExtractor;
import fr.esgi.kafka.relay.common.JsonSerdes;
import fr.esgi.kafka.relay.model.Campaign;
import fr.esgi.kafka.relay.model.CampaignGeoCount;
import fr.esgi.kafka.relay.model.CampaignImpression;
import fr.esgi.kafka.relay.model.CampaignSpend;
import fr.esgi.kafka.relay.model.Click;
import fr.esgi.kafka.relay.model.ClickBotAlert;
import fr.esgi.kafka.relay.model.ClickMatch;
import fr.esgi.kafka.relay.model.GeoCampaignTop;
import fr.esgi.kafka.relay.model.DlqRecord;
import fr.esgi.kafka.relay.model.Impression;
import fr.esgi.kafka.relay.model.SiteViewability;
import fr.esgi.kafka.relay.model.ViewabilityCount;
import fr.esgi.kafka.relay.processing.ImpressionDeduplicator;
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
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
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
    public static final Duration TOP_WINDOW = Duration.ofMinutes(15);
    public static final Duration TOP_ADVANCE = Duration.ofMinutes(5);
    /** Les retardataires arrivent jusqu'a 180 min apres leur horodatage. */
    public static final Duration LATE_EVENTS_GRACE = Duration.ofMinutes(180);
    /** Fenetre de la jointure flux-flux impression x clic. */
    public static final Duration CLICK_JOIN_WINDOW = Duration.ofMinutes(30);
    /** Memoire des impression_id deja factures : la retention du cluster partage. */
    public static final Duration DEDUP_WINDOW = Duration.ofHours(24);
    public static final String DEDUP_STORE = "spend-dedup-store";
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
        buildTopByGeo(builder, impressions);

        // RLY-4 - Depense par campagne                    -> Topics.SPEND
        buildSpend(builder, impressions);

        // RLY-5 - Tunnel impression -> clic, clics robots -> Topics.ALERTS_CLICKBOT
        buildClickbots(impressions, clicks);

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

    private static void buildTopByGeo(StreamsBuilder builder, KStream<String, Impression> impressions) {
        GlobalKTable<String, String> campaigns = builder.globalTable(
                Topics.CAMPAIGNS,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("campaigns-store"));

        impressions
                .leftJoin(campaigns,
                        (userId, imp) -> imp.campaignId(),
                        (imp, campaignJson) -> CampaignImpression.of(
                                imp, JsonSerdes.parseOrNull(campaignJson, Campaign.class)),
                        Named.as("impressions-join-campaigns"))
                .selectKey((userId, imp) -> imp.geo() + "|" + imp.campaignId(),
                        Named.as("impressions-by-geo-campaign"))
                .groupByKey(Grouped.with("top-by-geo", Serdes.String(), JsonSerdes.of(CampaignImpression.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(TOP_WINDOW, LATE_EVENTS_GRACE).advanceBy(TOP_ADVANCE))
                .aggregate(CampaignGeoCount::empty,
                        (geoCampaign, imp, count) -> count.add(imp),
                        Named.as("top-by-geo-count"),
                        Materialized.<String, CampaignGeoCount, WindowStore<Bytes, byte[]>>as("top-by-geo-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.of(CampaignGeoCount.class)))
                .toStream(Named.as("top-by-geo-updates"))
                .map((window, count) -> KeyValue.pair(count.geo(), JsonSerdes.toJson(new GeoCampaignTop(
                                count.geo(), count.campaignId(), count.name(), count.advertiser(), count.inCatalog(),
                                window.window().startTime().toString(), window.window().endTime().toString(),
                                count.impressions()))),
                        Named.as("top-by-geo-to-json"))
                .to(Topics.TOP_BY_GEO, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void buildSpend(StreamsBuilder builder, KStream<String, Impression> impressions) {
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(DEDUP_STORE, DEDUP_WINDOW.plus(LATE_EVENTS_GRACE), DEDUP_WINDOW, false),
                Serdes.String(), Serdes.Long()));

        impressions
                .selectKey((userId, imp) -> imp.impressionId(), Named.as("impressions-by-id"))
                .repartition(Repartitioned.<String, Impression>as("spend-dedup-by-id")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.of(Impression.class)))
                .processValues(() -> new ImpressionDeduplicator(DEDUP_STORE, DEDUP_WINDOW),
                        Named.as("spend-dedup"), DEDUP_STORE)
                .selectKey((impressionId, imp) -> imp.campaignId(), Named.as("impressions-by-campaign"))
                .groupByKey(Grouped.with("spend-by-campaign", Serdes.String(), JsonSerdes.of(Impression.class)))
                .aggregate(CampaignSpend::empty,
                        (campaignId, imp, spend) -> spend.add(campaignId, imp),
                        Named.as("spend-sum"),
                        Materialized.<String, CampaignSpend, KeyValueStore<Bytes, byte[]>>as("spend-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.of(CampaignSpend.class)))
                .toStream(Named.as("spend-updates"))
                .mapValues(spend -> JsonSerdes.toJson(spend.toReport()), Named.as("spend-to-json"))
                .to(Topics.SPEND, Produced.with(Serdes.String(), Serdes.String()));
    }

    private void buildClickbots(KStream<String, Impression> impressions, KStream<String, Click> clicks) {
        KStream<String, Impression> impressionsById = impressions
                .selectKey((userId, imp) -> imp.impressionId(), Named.as("join-impressions-key"))
                .repartition(Repartitioned.<String, Impression>as("join-impressions-by-id")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.of(Impression.class)));

        KStream<String, Click> clicksById = clicks
                .selectKey((userId, click) -> click.impressionId(), Named.as("join-clicks-key"))
                .repartition(Repartitioned.<String, Click>as("join-clicks-by-id")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.of(Click.class)));

        Duration botWindow = Duration.ofMinutes(botWindowMinutes);
        impressionsById
                .join(clicksById,
                        ClickMatch::from,
                        JoinWindows.ofTimeDifferenceAndGrace(CLICK_JOIN_WINDOW, LATE_EVENTS_GRACE),
                        StreamJoined.<String, Impression, Click>as("impression-click-join")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.of(Impression.class))
                                .withOtherValueSerde(JsonSerdes.of(Click.class)))
                .filter((impressionId, match) -> match.isBot(BOT_MAX_DELAY_MS), Named.as("bot-clicks-only"))
                .selectKey((impressionId, match) -> match.userId(), Named.as("bot-clicks-by-user"))
                .groupByKey(Grouped.with("bot-clicks", Serdes.String(), JsonSerdes.of(ClickMatch.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(botWindow, LATE_EVENTS_GRACE))
                .count(Named.as("bot-clicks-count"),
                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("bot-clicks-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()))
                .toStream(Named.as("bot-clicks-updates"))
                .filter((window, count) -> count >= BOT_MIN_CLICKS, Named.as("bot-alerts-only"))
                .map((window, count) -> KeyValue.pair(window.key(), JsonSerdes.toJson(new ClickBotAlert(
                                window.key(),
                                window.window().startTime().toString(),
                                window.window().endTime().toString(),
                                botWindowMinutes,
                                count))),
                        Named.as("bot-alerts-to-json"))
                .to(Topics.ALERTS_CLICKBOT, Produced.with(Serdes.String(), Serdes.String()));
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
