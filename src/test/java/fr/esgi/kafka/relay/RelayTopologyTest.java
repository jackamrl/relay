package fr.esgi.kafka.relay;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Squelette de tests unitaires de topologie (bonus RLY-6, vu en seance 14).
 * Outil : TopologyTestDriver (ajoutez kafka-streams-test-utils en scope
 * test dans le pom, version alignee sur le client Kafka du BOM Quarkus).
 * On teste la Topology nue (new TopologyProducer().buildTopology()),
 * pas besoin de @QuarkusTest ni de broker.
 */
class RelayTopologyTest {

    @Test
    @Disabled("A implementer : une impression invalide doit finir dans la DLQ")
    void impressionInvalideDoitPartirEnDlq() {
    }

    @Test
    @Disabled("A implementer : un doublon exact ne facture pas deux fois")
    void doublonNeFacturePasDeuxFois() {
    }

    @Test
    @Disabled("A implementer : un clic 300 ms apres l'impression est un robot, "
            + "un clic 30 s apres ne l'est pas")
    void clicTropRapideEstUnRobot() {
    }
}
