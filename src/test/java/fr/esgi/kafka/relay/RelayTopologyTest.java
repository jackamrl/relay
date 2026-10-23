package fr.esgi.kafka.relay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RLY-6. TopologyTestDriver execute la topologie en memoire : aucun broker,
 * aucun sleep. Le temps des fenetres est le timestamp passe a pipeInput,
 * qui est aussi ecrit dans le champ JSON "timestamp".
 */
class RelayTopologyTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant T0 = Instant.parse("2026-07-04T14:03:00Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> impressions;
    private TestInputTopic<String, String> clicks;
    private TestOutputTopic<String, String> dlq;
    private TestOutputTopic<String, String> spend;
    private TestOutputTopic<String, String> alerts;
    private WindowStore<String, Long> botClicks;

    @BeforeEach
    void setup() {
        TopologyProducer producer = new TopologyProducer();
        // Pas d'injection Quarkus ici : la fenetre anti-robot est posee a la main.
        producer.botWindowMinutes = 10;

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "relay-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Chaque mise a jour sort tout de suite : les assertions ne dependent pas du cache.
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        driver = new TopologyTestDriver(producer.buildTopology(), props);
        impressions = driver.createInputTopic(Topics.IMPRESSIONS, new StringSerializer(), new StringSerializer());
        clicks = driver.createInputTopic(Topics.CLICKS, new StringSerializer(), new StringSerializer());
        dlq = driver.createOutputTopic(Topics.DLQ, new StringDeserializer(), new StringDeserializer());
        spend = driver.createOutputTopic(Topics.SPEND, new StringDeserializer(), new StringDeserializer());
        alerts = driver.createOutputTopic(Topics.ALERTS_CLICKBOT, new StringDeserializer(), new StringDeserializer());
        botClicks = driver.getWindowStore("bot-clicks-store");
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void impressionInvalideDoitPartirEnDlq() throws Exception {
        String raw = "{"
                + "\"impression_id\":\"imp-bad\",\"user_id\":\"u-1\",\"campaign_id\":\"cmp-0001\","
                + "\"creative_id\":\"crea-1\",\"site_id\":\"site-001\",\"device\":\"MOBILE\",\"geo\":\"FR\","
                + "\"viewable\":true,\"paid_price_eur\":true,\"timestamp\":\"2026-07-04T14:03:21.512Z\""
                + "}";

        impressions.pipeInput("u-1", raw, T0);

        KeyValue<String, String> out = dlq.readKeyValue();
        assertTrue(dlq.isEmpty());
        assertEquals("u-1", out.key);
        JsonNode node = MAPPER.readTree(out.value);
        assertEquals("relay.impressions", node.get("source_topic").asText());
        assertEquals("BAD_TYPE", node.get("reason").asText());
        assertEquals(raw, node.get("raw").asText());
        assertTrue(node.get("detail").asText().contains("BOOLEAN"));
    }

    @Test
    void doublonNeFacturePasDeuxFois() throws Exception {
        pipeImpression("imp-1", "u-1", "cmp-0007", 0.5, T0);
        pipeImpression("imp-1", "u-1", "cmp-0007", 0.5, T0);
        pipeImpression("imp-2", "u-1", "cmp-0007", 0.5, T0.plusSeconds(1));

        List<JsonNode> updates = new ArrayList<>();
        while (!spend.isEmpty()) {
            updates.add(MAPPER.readTree(spend.readValue()));
        }
        JsonNode last = updates.get(updates.size() - 1);
        assertEquals("cmp-0007", last.get("campaign_id").asText());
        // imp-1 compte une fois, imp-2 une fois : 1.0 EUR, pas 1.5.
        assertEquals(2, last.get("impressions").asInt());
        assertEquals(1.0, last.get("spend_eur").asDouble(), 0.000001);
    }

    @Test
    void clicTropRapideEstUnRobot() throws Exception {
        pipePair("imp-human", "clk-human", "u-human", T0, T0.plusSeconds(30));
        pipePair("imp-bot-0", "clk-bot-0", "bot-1", T0, T0.plusMillis(300));

        assertEquals(0, botClicksOf("u-human"));
        assertEquals(1, botClicksOf("bot-1"));
        assertTrue(alerts.isEmpty());

        for (int i = 1; i < TopologyProducer.BOT_MIN_CLICKS; i++) {
            Instant at = T0.plusSeconds(i);
            pipePair("imp-bot-" + i, "clk-bot-" + i, "bot-1", at, at.plusMillis(300));
        }

        JsonNode alert = MAPPER.readTree(alerts.readValue());
        assertTrue(alerts.isEmpty());
        assertEquals("bot-1", alert.get("user_id").asText());
        assertEquals(10, alert.get("bot_clicks").asInt());
        assertEquals(10, alert.get("window_minutes").asInt());

        // Un clic horodate avant son impression a un delta negatif : ce n'est pas un robot.
        pipePair("imp-late", "clk-late", "bot-1", T0.plusSeconds(40), T0.plusSeconds(20));
        assertEquals(10, botClicksOf("bot-1"));
        assertEquals(0, botClicksOf("u-human"));
        assertTrue(alerts.isEmpty());
    }

    private void pipePair(String impressionId, String clickId, String user, Instant impressionAt, Instant clickAt) {
        pipeImpression(impressionId, user, "cmp-0001", 0.01, impressionAt);
        pipeClick(clickId, impressionId, user, clickAt);
    }

    private void pipeImpression(String id, String user, String campaign, double price, Instant ts) {
        String json = "{"
                + "\"impression_id\":\"" + id + "\",\"user_id\":\"" + user + "\","
                + "\"campaign_id\":\"" + campaign + "\",\"creative_id\":\"crea-1\","
                + "\"site_id\":\"site-001\",\"device\":\"MOBILE\",\"geo\":\"FR\","
                + "\"viewable\":true,\"paid_price_eur\":" + price + ","
                + "\"timestamp\":\"" + ts + "\"}";
        impressions.pipeInput(user, json, ts);
    }

    private void pipeClick(String clickId, String impressionId, String user, Instant ts) {
        String json = "{"
                + "\"click_id\":\"" + clickId + "\",\"impression_id\":\"" + impressionId + "\","
                + "\"user_id\":\"" + user + "\",\"campaign_id\":\"cmp-0001\","
                + "\"device\":\"MOBILE\",\"geo\":\"FR\",\"timestamp\":\"" + ts + "\"}";
        clicks.pipeInput(user, json, ts);
    }

    private long botClicksOf(String user) {
        long total = 0;
        try (WindowStoreIterator<Long> rows = botClicks.fetch(user, Instant.EPOCH, Instant.parse("2100-01-01T00:00:00Z"))) {
            while (rows.hasNext()) {
                total += rows.next().value;
            }
        }
        return total;
    }
}
