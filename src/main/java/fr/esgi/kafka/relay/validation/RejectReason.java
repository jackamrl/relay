package fr.esgi.kafka.relay.validation;

/** Motifs de rejet en DLQ : un code stable par famille d'anomalie. */
public enum RejectReason {
    EMPTY_MESSAGE,
    INVALID_JSON,
    NOT_A_JSON_OBJECT,
    MISSING_FIELD,
    NULL_FIELD,
    EMPTY_FIELD,
    BAD_TYPE,
    OUT_OF_RANGE,
    BAD_ENUM,
    BAD_TIMESTAMP,
    VALIDATION_ERROR
}
