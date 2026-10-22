package fr.esgi.kafka.relay.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.esgi.kafka.relay.model.Click;
import fr.esgi.kafka.relay.model.Impression;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.function.Function;

/**
 * Validation des messages bruts de relay.impressions et relay.clicks.
 * On travaille sur l'arbre JSON (JsonNode) et non sur le record : Jackson
 * convertirait silencieusement certains types ("0.5" ou "NaN" en Double,
 * "true" en Boolean), alors que le contrat exige le type JSON exact.
 */
public final class EventValidator {

    public static final Set<String> DEVICES = Set.of("TV", "MOBILE", "WEB", "CONSOLE");
    public static final Set<String> GEOS = Set.of("FR", "US", "DE", "BR", "JP", "GB");
    public static final double MIN_PRICE_EUR = 0.0;
    public static final double MAX_PRICE_EUR = 1.0;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private EventValidator() {
    }

    public static Validated<Impression> impression(String raw) {
        return validate(raw, node -> new Impression(
                requiredText(node, "impression_id"),
                requiredText(node, "user_id"),
                requiredText(node, "campaign_id"),
                requiredText(node, "creative_id"),
                requiredText(node, "site_id"),
                requiredEnum(node, "device", DEVICES),
                requiredEnum(node, "geo", GEOS),
                requiredBoolean(node, "viewable"),
                requiredNumber(node, "paid_price_eur", MIN_PRICE_EUR, MAX_PRICE_EUR),
                requiredTimestamp(node, "timestamp")));
    }

    public static Validated<Click> click(String raw) {
        return validate(raw, node -> new Click(
                requiredText(node, "click_id"),
                requiredText(node, "impression_id"),
                requiredText(node, "user_id"),
                requiredText(node, "campaign_id"),
                requiredEnum(node, "device", DEVICES),
                requiredEnum(node, "geo", GEOS),
                requiredTimestamp(node, "timestamp")));
    }

    private static <T> Validated<T> validate(String raw, Function<JsonNode, T> mapper) {
        if (raw == null || raw.isBlank()) {
            return Validated.ko(raw, RejectReason.EMPTY_MESSAGE, "message vide");
        }
        JsonNode node;
        try {
            node = MAPPER.readTree(raw);
        } catch (JsonProcessingException e) {
            return Validated.ko(raw, RejectReason.INVALID_JSON,
                    "JSON illisible : " + e.getOriginalMessage());
        }
        if (node == null || !node.isObject()) {
            return Validated.ko(raw, RejectReason.NOT_A_JSON_OBJECT,
                    "objet JSON attendu, recu : " + (node == null ? "rien" : node.getNodeType()));
        }
        try {
            return Validated.ok(raw, mapper.apply(node));
        } catch (Rejection r) {
            return Validated.ko(raw, r.reason, r.getMessage());
        } catch (RuntimeException e) {
            return Validated.ko(raw, RejectReason.VALIDATION_ERROR, String.valueOf(e));
        }
    }

    private static JsonNode present(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null) {
            throw new Rejection(RejectReason.MISSING_FIELD, "champ requis absent : " + field);
        }
        if (value.isNull()) {
            throw new Rejection(RejectReason.NULL_FIELD, "champ requis a null : " + field);
        }
        return value;
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = present(node, field);
        if (!value.isTextual()) {
            throw new Rejection(RejectReason.BAD_TYPE,
                    field + " doit etre une chaine, recu " + value.getNodeType() + " : " + value);
        }
        if (value.textValue().isBlank()) {
            throw new Rejection(RejectReason.EMPTY_FIELD, field + " est vide");
        }
        return value.textValue();
    }

    private static String requiredEnum(JsonNode node, String field, Set<String> allowed) {
        JsonNode value = present(node, field);
        if (!value.isTextual()) {
            throw new Rejection(RejectReason.BAD_TYPE,
                    field + " doit etre une chaine, recu " + value.getNodeType() + " : " + value);
        }
        if (!allowed.contains(value.textValue())) {
            throw new Rejection(RejectReason.BAD_ENUM,
                    field + " inconnu : \"" + value.textValue() + "\" (attendu " + allowed + ")");
        }
        return value.textValue();
    }

    private static Boolean requiredBoolean(JsonNode node, String field) {
        JsonNode value = present(node, field);
        if (!value.isBoolean()) {
            throw new Rejection(RejectReason.BAD_TYPE,
                    field + " doit etre un booleen, recu " + value.getNodeType() + " : " + value);
        }
        return value.booleanValue();
    }

    private static Double requiredNumber(JsonNode node, String field, double min, double max) {
        JsonNode value = present(node, field);
        if (!value.isNumber()) {
            throw new Rejection(RejectReason.BAD_TYPE,
                    field + " doit etre un nombre, recu " + value.getNodeType() + " : " + value);
        }
        double d = value.doubleValue();
        if (!Double.isFinite(d) || d < min || d > max) {
            throw new Rejection(RejectReason.OUT_OF_RANGE,
                    field + " hors bornes [" + min + ", " + max + "] : " + value);
        }
        return d;
    }

    private static String requiredTimestamp(JsonNode node, String field) {
        String text = requiredText(node, field);
        try {
            Instant.parse(text);
        } catch (DateTimeParseException e) {
            throw new Rejection(RejectReason.BAD_TIMESTAMP,
                    field + " n'est pas un ISO-8601 valide : \"" + text + "\"");
        }
        return text;
    }

    /** Rejet metier : sans stack trace, c'est un cas attendu, pas un bug. */
    private static final class Rejection extends RuntimeException {
        private final RejectReason reason;

        Rejection(RejectReason reason, String detail) {
            super(detail, null, false, false);
            this.reason = reason;
        }
    }
}
