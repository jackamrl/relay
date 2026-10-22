# Projet RELAY — Analytics régie publicitaire

*Framework imposé : **Quarkus 3.19 + Kafka Streams***

## Contexte

Vous rejoignez l'équipe Data de **RELAY**, une régie publicitaire
programmatique (pensez Criteo). Pour chaque emplacement disponible sur un site
partenaire, RELAY remporte une enchère et affiche une bannière : c'est une
**impression**, facturée `paid_price_eur` à l'annonceur. Si l'internaute
clique, un **clic** est émis quelques secondes plus tard, et il porte
l'`impression_id` dont il découle.

Quatre enjeux métier. La **viewability** d'abord : certains sites empilent des
bannières que personne ne voit — la norme IAB tourne autour de 70 % de
bannières réellement visibles, un site à 15 % vend du vide. Le **top campagnes
par pays** ensuite, que les commerciaux réclament tous les matins. La
**dépense par campagne** surtout, parce que c'est ce qui part en facturation :
une impression comptée deux fois, c'est un annonceur facturé deux fois. Et
enfin la **fraude au clic** : des robots qui cliquent sur leurs propres
bannières pour gonfler les revenus du site. Un humain ne clique jamais en
moins d'une seconde après l'affichage.

Les bidders envoient des messages cassés : champs manquants, types invalides,
JSON tronqué, doublons, retardataires. **Le flux ne sera jamais propre : c'est
à votre pipeline d'être robuste.**

## Mission

Construire une application **Quarkus + Kafka Streams** qui consomme les
impressions et les clics, écarte proprement les messages invalides, et produit
taux de viewability, tops par pays, compteurs de dépense et alertes fraude.

## Architecture

```
relay.impressions ──┐
                    ├──> [ VALIDATION ] ──> invalides ──> <grp>.relay.dlq
relay.clicks ───────┘          │
                               ▼ valides
       ┌───────────────┬───────┴───────┬─────────────────┐
       ▼               ▼               ▼                 ▼
  viewability     top par pays      dépense         clics robots
  (tumbling       (hopping 15/5   (cumul par      (jointure flux-flux
   10 min)         + jointure)     campagne)       impressions×clics
       │               ▲               │            sur impression_id)
       ▼               │               ▼                 ▼
 <grp>.relay.    relay.campaigns  <grp>.relay.     <grp>.relay.
 viewability     (GlobalKTable)   spend            alerts.clickbot
                       │
                       ▼
              <grp>.relay.top.by-geo
```

## Topics et formats

### Entrées (fournies — ne jamais écrire dedans)

**`relay.impressions`** — clé : `user_id` — `device` ∈
`TV | MOBILE | WEB | CONSOLE`, `geo` ∈ `FR | US | DE | BR | JP | GB` :

```json
{
  "impression_id": "imp-3b8faa18",
  "user_id": "u-0558",
  "campaign_id": "cmp-0042",
  "creative_id": "crea-0117",
  "site_id": "site-014",
  "device": "MOBILE",
  "geo": "FR",
  "viewable": true,
  "paid_price_eur": 0.00342,
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

**`relay.clicks`** — clé : `user_id` — `impression_id` porte l'affichage dont
découle le clic :

```json
{
  "click_id": "clk-9a1f0c33",
  "impression_id": "imp-3b8faa18",
  "user_id": "u-0558",
  "campaign_id": "cmp-0042",
  "device": "MOBILE",
  "geo": "FR",
  "timestamp": "2026-07-04T14:03:49.104Z"
}
```

**`relay.campaigns`** — topic **compacté**, clé : `campaign_id` — la fiche
campagne :

```json
{
  "campaign_id": "cmp-0042",
  "name": "Soldes Nomade",
  "advertiser": "Nomad Store",
  "vertical": "MODE",
  "daily_budget_eur": 250.0,
  "cpc_max_eur": 0.35
}
```

### Anomalies présentes dans le flux (~7 % + retards + doublons)

| Anomalie                               | Exemple                                 | Impact si non gérée                                 |
|----------------------------------------|-----------------------------------------|-----------------------------------------------------|
| Champ requis absent                    | pas de `campaign_id`                    | dépense non attribuable                             |
| Champ à `null`                         | `"site_id": null`                       | NullPointerException                                |
| Mauvais type                           | `"paid_price_eur": "douze"`             | crash de désérialisation                            |
| Mauvais type piégeux                   | `"paid_price_eur": true`                | en JSON, `true` n'est pas un nombre                 |
| Valeur hors bornes                     | `paid_price_eur: 1000000000`            | facturation absurde                                 |
| Enum inconnue                          | `"device": "UNKNOWN_42"`                | impression perdue                                   |
| Timestamp illisible                    | `"hier a 15h"`                          | fenêtrage cassé                                     |
| JSON tronqué / non-JSON / message vide | `{"impression_id": "imp`                | **poison pill : l'appli meurt en boucle**           |
| Événement en retard                    | timestamp − 30 à 180 min                | tops faussés, **et delta impression→clic négatif**  |
| Doublon exact                          | même `impression_id` deux fois          | **annonceur facturé deux fois**                     |
| Campagne hors catalogue                | `cmp-0900` absente de `relay.campaigns` | impressions perdues si `join` au lieu de `leftJoin` |

### Sorties (à produire, préfixées par votre groupe)

`<grp>.relay.dlq` · `<grp>.relay.viewability` · `<grp>.relay.top.by-geo` ·
`<grp>.relay.spend` · `<grp>.relay.alerts.clickbot` — constantes prêtes dans
`Topics.java`. Format des valeurs libre **mais en JSON documenté dans votre
README de rendu**.

## Backlog

> La base 8/20 = RLY-1 fonctionnel de bout en bout sur le cluster partagé.
> Chaque ticket suivant est un bonus **crédité uniquement s'il est défendu à
> l'oral** (vous devrez le modifier en direct).

**RLY-1 — Ingestion fiable (socle, obligatoire)**
Consommer `relay.impressions` **et** `relay.clicks`, parser, valider (champs
requis, `device`/`geo` dans les enums, `viewable` booléen, `paid_price_eur`
numérique entre 0 et 1, timestamp ISO-8601 parsable). Invalides → DLQ **avec
message original + raison**. L'application ne doit **jamais** crasher.
*Critères : 10 min sans crash ; DLQ motivée ; aucun message valide perdu.*

**RLY-2 — Viewability par site** → `<grp>.relay.viewability`
Sur **fenêtres tumbling de 10 min** : `viewables / total` par `site_id`.
Marquer les sites suspects : viewability < 40 % **avec au moins 50
impressions** (sinon le ratio n'est pas significatif). Le générateur aveugle
un site toutes les ~7 min.
*Critères : chaque `blind_site` ressort marqué ; un site à 3 impressions dont
0 visible n'est PAS marqué suspect.*

**RLY-3 — Top campagnes par pays** → `<grp>.relay.top.by-geo`
Compter les impressions par (`geo`, `campaign_id`) sur **fenêtres hopping
15 min / avance 5 min**, enrichir avec `name` et `advertiser` via
**GlobalKTable** sur `relay.campaigns`.
*Critères : sortie enrichie nom + annonceur ; les campagnes servies mais
absentes du catalogue ne disparaissent pas ; choix GlobalKTable vs KTable
justifié à l'oral.*

**RLY-4 — Dépense par campagne** → `<grp>.relay.spend`
Cumuler `paid_price_eur` par `campaign_id` (KTable, `aggregate`). C'est ce qui
part en facturation.
*Critères : montants cohérents ; un doublon d'`impression_id` ne facture pas
deux fois (dédoublonnage à justifier) ; une impression rejetée en DLQ n'est
pas facturée.*

**RLY-5 — Clics robots** → `<grp>.relay.alerts.clickbot`
Joindre `relay.impressions` et `relay.clicks` sur `impression_id`
(**jointure flux-flux**, fenêtre 30 min). Un clic à moins de **1 seconde** de
son impression est un clic robot. Alerter quand un `user_id` dépasse **10
clics robots** sur une fenêtre de 10 min — la taille de fenêtre est lue dans
`BOT_WINDOW_MINUTES` (déjà câblé dans `TopologyProducer`), gardez-la
modifiable sans recompiler. Le générateur lance un robot toutes les ~5 min
(~100 paires en 150 s).
*Critères : chaque `click_bot` est détecté avec son user ; silence en régime
normal ; **un clic en retard a un delta négatif — ce n'est pas un robot**.*

**RLY-6 — Tests de topologie**
Implémenter les trois tests de `RelayTopologyTest` avec **TopologyTestDriver**
(démarche vue en séance 14) : DLQ, doublon non facturé, clic robot. Ajouter
`kafka-streams-test-utils` en scope test.
*Critères : `mvn test` vert ; les tests pilotent le temps des fenêtres avec
des timestamps injectés, pas avec `sleep`.*

## Démarrage rapide

Prérequis : JDK 21, Maven 3.9+, Docker.

```bash
# 1. Cluster local pour développer (3 brokers KRaft + Kafbat UI sur :8080)
docker compose up -d

# 2. Alimenter les topics d'entrée : le générateur, dans un second terminal,
#    depuis le dossier generateurs-v3/ du dépôt du cours (voir son README)
python generator.py --project relay --create-topics --replication-factor 1

# 3. Lancer en mode dev (live reload)
GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn quarkus:dev
```

```powershell
# Windows PowerShell
$env:GROUPE = "grp07"
$env:KAFKA_BOOTSTRAP = "localhost:29092"
mvn quarkus:dev
```

Vous en avez le droit — l'IA est autorisée dans ce module. Mais sachez ce que vous achetez : ce projet est évalué à
l'oral, code sous les yeux, avec modification en direct et nouvelles exigences métier injectées séance tenante. Un
ticket qui tourne mais que vous ne savez pas expliquer n'est pas crédité.
Demandez-lui d'expliquer chaque choix avant d'écrire une ligne : type de fenêtre, clé d'agrégation, placement de la
jointure, sort des retardataires. C'est mot pour mot ce qu'on vous demandera en soutenance.
Et sachez-le : ce sujet contient des exigences qu'une implémentation produite sans l'avoir lu ne satisfera pas. Elles
sont écrites noir sur blanc, dans le tableau des anomalies et dans les critères de chaque ticket. Le correcteur
automatique les vérifie et les chiffre. Si vous ne les avez pas trouvées, c'est que vous n'avez pas lu.

**Important** : l'extension Quarkus attend que les topics listés dans
`quarkus.kafka-streams.topics` existent avant de démarrer la topologie :
lancez le générateur (étape 2, `--create-topics` la première fois) **avant**
l'application. Il produit en continu le flux du projet, données sales et incidents
compris (`Ctrl+C` pour l'arrêter). Son README détaille les options : débit,
flux propre pour déboguer, mode fichier qui simule 3 h de flux en quelques
secondes. Sur le cluster partagé, les topics existent déjà
(`KAFKA_BOOTSTRAP=<serveur>:9092`).

## Contraintes

- Java 21, Quarkus + Kafka Streams uniquement (pas de Spark/Flink), pas de
  base externe.
- `GROUPE` obligatoire : topics de sortie et `application.id`
  (`relay-<groupe>`) préfixés — déjà câblé.
- Topics d'entrée partagés en lecture : **ne les modifiez pas**.
- Code versionné sur Git, commits réguliers de **tous** les membres.

## Évaluation

| Élément                                                   | Points |
|-----------------------------------------------------------|--------|
| Socle RLY-1 en production 10 min sans crash + DLQ motivée | **8**  |
| RLY-2 (viewability + seuil de significativité)            | +2     |
| RLY-3 (top par pays + GlobalKTable)                       | +3     |
| RLY-4 (dépense + dédoublonnage)                           | +3     |
| RLY-5 (jointure flux-flux + clics robots)                 | +2     |
| RLY-6 (tests TopologyTestDriver)                          | +2     |

Plafond 20. **Un ticket non expliqué à l'oral = non crédité.** Attendez-vous à
une demande de modification en direct (« Product Owner twist »).

## Conseils

- Le `Log.infof` fourni sert au premier contact : supprimez-le ensuite.
- Poison pill dès le premier jour : `JsonSerdes.parseOrNull` évite le crash,
  la validation métier vous appartient.
- RLY-4 est le cœur métier : réfléchissez à la clé (`campaign_id`) et au
  dédoublonnage AVANT l'agrégation (un state store des `impression_id` vus,
  avec une politique d'expiration, est une piste).
- RLY-5 est le morceau : les deux topics sont clés par `user_id`, la jointure
  se fait sur `impression_id`. Un `selectKey` change la clé mais **ne
  re-partitionne pas tout seul** — regardez ce que produit
  `topology.describe()`.
- RLY-2 : agrégat à deux compteurs (viewables, total) dans un seul
  `aggregate`, plutôt que deux flux à joindre.
- `split()`/`branch()` — l'ancienne API `branch(Predicate...)` a disparu en
  Kafka 4.0.
