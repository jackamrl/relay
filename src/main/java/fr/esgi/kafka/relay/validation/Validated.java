package fr.esgi.kafka.relay.validation;

/**
 * Resultat de la validation d'un message brut : soit l'evenement type,
 * soit le motif du rejet. Le message original est conserve dans les deux
 * cas pour pouvoir l'envoyer tel quel en DLQ.
 */
public record Validated<T>(T value, String raw, RejectReason reason, String detail) {

    public static <T> Validated<T> ok(String raw, T value) {
        return new Validated<>(value, raw, null, null);
    }

    public static <T> Validated<T> ko(String raw, RejectReason reason, String detail) {
        return new Validated<>(null, raw, reason, detail);
    }

    public boolean isValid() {
        return reason == null;
    }
}
