# Choix techniques

Choix d'implémentation du socle (phase 1), du générateur (phase 2, décisions H1 à H22), du
premier consommateur Spark (phase 3, décisions I1 à I8) et des règles R2 et R3 (phase 4,
décisions J1 à J12), en complément du cadrage de la phase 0 (décisions F1 à F22).

## PostgreSQL

| Choix | Pourquoi |
|---|---|
| Port hôte **5434** | 5432 est pris par le PostgreSQL local de Windows, 5433 par le projet 1 : les deux projets peuvent tourner en même temps |
| **CHECK sur `transactions`** (listes de types, canaux et régions, formats d'identifiants, matrice type × canal, destinataire selon le type, montant de 100 à 2 000 000) | Dernier filet ; la validation se fait dans Spark avant l'écriture, sinon une ligne invalide fait échouer tout le micro-batch (phase 3) |
| `unique (kafka_partition, kafka_offset)` sur `transactions` | Un message Kafka ne donne qu'une transaction |
| `sql/init.sql` exécuté à la création du volume seulement | Comportement de l'image `postgres` ; `make reset` supprime le volume pour le rejouer |

## Kafka

| Choix | Pourquoi |
|---|---|
| Création automatique des topics désactivée ; topics créés par `kafka-init` avec `--if-not-exists` | Un nom de topic mal orthographié échoue au lieu de créer un topic à 1 partition ; la création est rejouable à chaque `up` |
| Volume nommé et `CLUSTER_ID` fixe | Les topics survivent à `make down` ; `make reset` repart de zéro |
| Tas réduit (`-Xms32m -Xmx128m`) pour le healthcheck, `make topics` et le test de charge | `KAFKA_HEAP_OPTS` (512 Mo) serait hérité par chaque outil Kafka : chaque contrôle réserverait 512 Mo de plus et fausserait `docker stats` |
| Test de charge dans un conteneur à part | Sa JVM n'est pas comptée dans la mémoire du broker |

## Configuration et exposition

| Choix | Pourquoi |
|---|---|
| Variables préfixées **`SENPAY_`** (`SENPAY_PG_PORT`, `SENPAY_PG_USER`…) | Docker Compose et `uv run --env-file` donnent la priorité aux variables du shell sur le fichier : un `POSTGRES_PORT=5433` exporté pour le projet 1 a écrasé le `.env` (vérifié pour les deux outils). Le préfixe évite la collision |
| `make` échoue si une variable `POSTGRES_*` ou `SENPAY_*` est exportée dans le shell | Le préfixe ne protège pas d'un `SENPAY_*` exporté : même mécanisme, même garde-fou. Seuls les noms sont affichés, jamais les valeurs |
| Ports publiés sur **`127.0.0.1`** seulement (PostgreSQL et Kafka), listener Kafka annoncé sur `127.0.0.1` | Mots de passe d'exemple et Kafka sans authentification : rien ne doit être joignable depuis le réseau local |
| `make check` compare les ports publiés (`docker inspect`) au `.env` | Exactement une liaison, sur 127.0.0.1, au port attendu : un port écrasé ou exposé sur `0.0.0.0` est un écart, pas un succès silencieux |

## Outillage

| Choix | Pourquoi |
|---|---|
| `make up` attend la fin de `kafka-init` avec `docker wait` | `docker compose up --wait` rend la main dès que `kafka-init` a démarré, et un conteneur encore en cours renvoie le code 0 : faux positif observé au premier essai |
| `scripts/check_infra.py` : lecture des services séparée de la comparaison | La comparaison est testée par pytest sans Kafka ni PostgreSQL ; codes de sortie 1 (écart) et 2 (service injoignable ou configuration manquante) |

## Générateur (phase 2)

### Architecture et reproductibilité

| # | Choix | Pourquoi |
|---|---|---|
| H1 | Le « quoi » est séparé du « quand » : un cœur de fonctions pures produit un flux déterministe d'émissions triées par instant d'émission ; la cadence et l'envoi (Kafka, PostgreSQL) sont une couche à part. Mode `--dry-run --out` sans Kafka ni base | Le contenu d'un run ne dépend que de la graine et des paramètres, pas de la vitesse ni de la machine ; tout se teste sans Docker et la reproductibilité se prouve par un sha256 |
| H2 | Génération par tranche d'une heure simulée ; un tas (`heapq`) garde les messages retardés jusqu'à leur instant d'émission | Mémoire bornée même pour 30 jours (≈ 900 000 messages) ; les retardataires sortent au bon moment, l'ordre global reste trié |
| H3 | Horloge `SimClock(sim_start, speed, real_start)` : `real_deadline(t) = real_start + (t − sim_start) / speed`. L'heure réelle est injectée (paramètre `now`), lue en un seul point d'entrée | Horloge testable sans `sleep` ; un seul endroit où le temps réel entre dans le code |
| H4 | Graine : un `random.Random("{seed}:{flux}")` par flux (population, trafic, épisodes, retards, invalides, trafic près des seuils). UUID tirés du générateur (`uuid.UUID(int=…, version=4)`), jamais `uuid.uuid4()` | `uuid4()` lit l'aléa du système : il casserait la reproductibilité sans erreur. Une graine texte est hachée de façon stable, indépendamment de `PYTHONHASHSEED` |
| H19 | Un flux aléatoire **par type d'épisode** (`{seed}:episodes:{TYPE}`) | Ajouter un type (R4, R5) ou changer le nombre de rafales ne modifie aucun autre épisode d'une même graine : un test compare les épisodes R1 à R3 de la graine 42 à une empreinte figée avant l'ajout de R4 et R5 |

### Plage simulée et vitesse

| # | Choix | Pourquoi |
|---|---|---|
| H5 | Plage explicite (`--start`, `--duration`). Refus si la plage finit après l'heure réelle, ou si `--start` précède la fin du dernier run en base | L'horloge simulée ne dépasse jamais l'heure réelle ; rejouer une plage antérieure impose une réinitialisation (`make reset`), sinon le watermark ignorerait tout |
| H6 | `--speed 60` (cadencé) ou `--speed max` (sans attente), enregistré `speed = null` | `max` produit des jours d'historique en quelques secondes ; un facteur numérique n'aurait pas de sens |

### Population et trafic

| # | Choix | Pourquoi |
|---|---|---|
| H7 | 10 000 clients (région habituelle pondérée, activité log-normale, paramètre de montant propre à chacun), 300 agents rattachés à une région (au moins un par région), 200 marchands | Un client a un comportement stable, ce qui rend la règle du montant habituel pertinente |
| H8 | 30 000 transactions par jour, processus de Poisson à intensité horaire (creux la nuit), mix de types et de canaux pondéré, région habituelle à 95 %. Même loi log-normale pour toutes les sorties d'un client ; `CASH_IN` a la sienne | Une seule loi par client pour les sorties : la médiane d'amorçage est exacte |
| H9 | Profils d'amorçage (`--init-profiles`, source `GENERATEUR`) : médiane = `exp(mu)`, `nb_tx_30j` = espérance arrondie des sorties sur 30 jours | Cohérents avec le trafic par construction ; c'est un amorçage, l'historique d'un ancien système, remplacé par le batch en phase 7 |
| H10 | Trafic normal près des seuils, absent de la vérité terrain : presque-rafales de 5 transferts, gros montants de jour, montants de nuit entre 400 000 et 500 000, sorties à 6–9 × la médiane | Les faux positifs doivent rester possibles (5 transferts « presque rafale » + 1 transfert ordinaire = 6) ; s'y ajoutent ceux de la queue des lois log-normales |

### Épisodes et vérité terrain

| # | Choix | Pourquoi |
|---|---|---|
| H11 | Épisodes générés à part du trafic, fusionnés par temps ; nombre par jour en configuration, instants tirés par un processus de Poisson sur la fenêtre horaire autorisée du type | ≈ 1 % des transactions ; un run d'une heure a peu d'épisodes, et aucun de nuit s'il ne couvre pas la nuit |
| H12 | Une transaction d'épisode n'est jamais invalide ni très en retard ; le retard ordinaire (≤ 5 min) reste possible | Sinon une non-détection viendrait du générateur, pas de la règle ; le retard ordinaire teste vraiment le watermark |
| H13 | Vérité terrain écrite **avant** l'envoi, une transaction PostgreSQL par tranche ; `sim_end` et `ended_at` posés seulement après un `flush()` Kafka sans erreur. Un run sans `ended_at` bloque les suivants (`make reset`) | Une transaction injectée envoyée sans vérité terrain deviendrait un faux positif invisible ; l'inverse (vérité écrite, envoi interrompu) se voit : run inachevé |

Épisodes : rafale (6 à 10 transferts en 90 s au plus), montant anormal (15 à 50 × la médiane,
de jour), nuit et gros montant (500 001 à 2 000 000 XOF entre 1 h et 5 h), cash-out rapide
(dépôt puis retrait d'au moins 90 % en 1 à 9 min, même région), changement de région (deux
sorties à moins de 15 min dans deux régions éloignées : Ziguinchor avec chacune des 5 autres,
et Saint-Louis–Kaolack). Les deux derniers sont injectés mais pas détectés au MVP.

### Retards, invalides, envoi

| # | Choix | Pourquoi |
|---|---|---|
| H14 | Retard = délai d'**émission** en temps simulé : 5 % de 10 s à 5 min, 0,2 % de 15 à 30 min ; `event_time` inchangé | Un message en retard part après d'autres plus récents : c'est ce que le watermark doit gérer |
| H15 | 0,5 % des transactions normales remplacées par une version corrompue (la version valide n'est pas envoyée), 8 motifs ; le motif « `event_time` dans le futur » est calculé à l'envoi avec l'horloge injectée | Chaque motif viole une règle de validation du schéma v1 ; la file des messages rejetés (phase 6) sera comparée motif par motif. `nb_sent` = valides + invalides |
| H16 | Producteur `acks=all`, idempotence activée, `linger.ms=20`, clé = `sender_id` ; horodatage posé par le client à l'envoi ; toute erreur de livraison fait échouer le run | `ingested_at` = horodatage Kafka en temps réel, base de la latence ; une erreur ignorée fausserait `nb_sent` |
| H17 | `generator_runs.params` (configuration complète, graine, options), `speed` nullable, `nb_invalid_by_reason` ; générateur lancé sur l'hôte (`make generate`) ; configuration TOML en dataclasses figées, validée au chargement, séparée des seuils des règles | Un run est rejouable depuis la base ; pas de dépendance ajoutée ; on peut changer un seuil de règle sans toucher au générateur |
| H18 | Le générateur refuse de démarrer si le run précédent n'a pas d'`ended_at` | Un run inachevé laisse une vérité terrain incomplète ; il faut repartir de `make reset` |
| H20 | Chaque transaction d'épisode est rangée dans la tranche horaire de son propre `event_time` | Une rafale peut donc chevaucher une heure pleine ; sinon aucune rafale ne traverserait jamais une heure pile, un biais discret (test `test_rafale_a_cheval_sur_deux_heures`) |

### Outillage du générateur

| # | Choix | Pourquoi |
|---|---|---|
| H21 | ruff garde RUF001–RUF003 actifs et n'autorise que ×, − et – (`allowed-confusables`) | Un caractère ambigu dans une chaîne reste détecté |
| H22 | Les tests d'intégration écrivent un run en base : ils sont exclus de `make test` et lancés par `make test-integration` | Pour ne pas bloquer un run de référence ultérieur |

## Consommateur Spark (phase 3)

### Décisions

| # | Choix | Pourquoi |
|---|---|---|
| I1 | Job dans un conteneur `spark-streaming` (profil `streaming`, image officielle `apache/spark:4.2.1-scala2.13-java17-python3-ubuntu` + psycopg) ; tests sur l'hôte avec `pyspark==4.2.1` et un JDK 17 | Mémoire isolée et mesurable (`docker stats`), réseau interne `kafka:29092` ; sur l'hôte, il faudrait Java de toute façon, sans isolation mémoire |
| I2 | `spark.jars.packages` avec coordonnées épinglées (connecteur Kafka 4.2.1, pilote JDBC 42.7.14) ; cache Ivy dans un volume nommé | Les dépendances transitives du connecteur (`kafka-clients`, `commons-pool2`…) sont résolues automatiquement ; copier les jars à la main, c'est risquer d'en oublier un |
| I3 | Validation en **expressions Spark natives, sans UDF Python** ; un motif par message, dans le même ordre de priorité que le validateur Python de référence ; **test de parité** sur les 30 530 messages du jour de référence | Une UDF Python fait passer chaque ligne de la JVM vers Python : lent et coûteux en mémoire. La parité prouve les motifs au lieu de les supposer |
| I4 | Écriture idempotente dès la phase 3 : JDBC vers `transactions_staging` (avec `batch_id`), puis, depuis le driver et en une transaction psycopg, `INSERT … SELECT … ON CONFLICT (transaction_id) DO NOTHING` et suppression du lot | `foreachBatch` est *at-least-once* : au premier arrêt brutal, le rejeu violerait la clé primaire et le job planterait à chaque relance. Le JDBC de Spark ne sait pas faire d'upsert |
| I5 | Table **`stream_batches`** (bilan de chaque micro-batch : lus, valides, insérés, invalides par motif), écrite une seule fois (`ON CONFLICT (batch_id) DO NOTHING`) **dans la même transaction que la fusion**. **Ajout au schéma du PRD §6.2** | Un compte d'invalides durable, fait par le consommateur et comparable motif par motif au générateur ; servira au débit dans Grafana. Un bilan présent prouve que le lot a été fusionné : un rejeu n'écrase jamais `nb_inseres` (invariant Σ `nb_inseres` = `COUNT(transactions)`, vérifié par `make stream-report`). La file `dead_letter` reste pour la phase 6 |
| I6 | Checkpoint dès la phase 3, dans un volume nommé, supprimé par `make reset` ; `startingOffsets=earliest` | Un checkpoint qui survit à un reset reprendrait à des offsets qui n'existent plus |
| I7 | `local[4]`, driver 2 Go, `shuffle.partitions=4`, `maxOffsetsPerTrigger=5000`, déclencheur de 5 s, `session.timeZone=Africa/Dakar`, limite du conteneur 3 Go | Budget mémoire du PRD §8.4 ; la limite empêche Spark de prendre toute la mémoire de WSL2 |
| I8 | Deux runs de contrôle : A, arriéré (run de référence en `--speed max`, puis démarrage du job) ; B, direct (job allumé, `--speed 60`, plage postérieure à A) | A prouve le rattrapage, B le vrai streaming, dans la même session et sans reset |

### Choix d'implémentation

| Choix | Pourquoi |
|---|---|
| Le JSON est lu deux fois par `from_json` : tous les champs **en texte**, avec la colonne d'enregistrement corrompu, puis les deux entiers (`schema_version`, `amount_xof`) typés, un par appel | Avec des champs typés, un montant en texte remplirait aussi la colonne d'enregistrement corrompu et passerait pour un JSON illisible (prouvé par un test). La chaîne vide ne remplit pas cette colonne : elle est attrapée par `json_object_keys`, qui renvoie `null` pour tout ce qui n'est pas un objet JSON |
| Motifs `CHAMP_INCONNU` et `FORMAT` en plus des 8 du générateur | Tout message hors schéma doit être écarté avant l'écriture : sinon un CHECK de `transactions` ferait échouer tout le micro-batch |
| Fonctions `try_*` (`try_to_timestamp`) et conditions « conforme » converties avec `coalesce(…, false)` | Le mode ANSI de Spark 4 lève une erreur sur une conversion impossible ; en SQL, une comparaison avec `null` donne `null`, qui doit compter comme un échec |
| Le code du job (`streaming/*.py`) est vérifié en **Python 3.10** (ruff `per-file-target-version`, second passage de mypy) et importé avec le `python3` de l'image (`make check-image`, appelé par `make check`) ; ses tests tournent sur l'hôte en 3.12 | L'image Spark embarque Python 3.10 : une construction de 3.11+ (`datetime.UTC`, `tomllib`…) planterait le job au démarrage. Les tests importent le générateur, qui utilise `tomllib` |
| `make up` ne démarre que Kafka et PostgreSQL ; le job démarre par `make stream` | Le run A exige de démarrer le job après coup, sur un arriéré |
| Interface web de Spark désactivée (`spark.ui.enabled=false`) | Mémoire et port inutiles pour un job sans opérateur |
| Le générateur refuse une graine déjà présente dans `generator_runs` (message : faire `make reset`) ; la dérivation des identifiants est inchangée | Les `transaction_id` sont tirés du flux « trafic » de la graine depuis son début : une même graine redonnerait les mêmes identifiants, que la fusion écarterait en silence comme déjà présents |

## Règles R2 et R3 (phase 4)

| # | Choix | Pourquoi |
|---|---|---|
| J1 | Seuils dans `config/rules.toml`, lus avec `tomllib` si disponible, sinon `tomli` (2.4.1, épinglé dans l'image) ; clé manquante, inconnue ou valeur invalide = arrêt du job au démarrage | L'image est en Python 3.10, qui n'a pas `tomllib` : sans repli, le job planterait dans le conteneur alors que les tests passent sur l'hôte en 3.12 |
| J2 | R2 et R3 sont appliquées dans `foreachBatch`, sur le lot validé ; `user_profiles` est **relu par JDBC à chaque lot**, sans cache. **Écart au PRD §7.1** : la technique passe de « jointure stream-statique » à « jointure lot ↔ instantané des profils » | Même comportement, mais la fraîcheur des profils ne dépend plus d'un `.cache()` posé par erreur : elle est explicite et testée (J12). C'est la condition pour que les profils recalculés la nuit (phase 7) soient vus |
| J3 | `streaming/rules.py` : `rule_r2` et `rule_r3` en expressions Spark natives, sans UDF, qui renvoient un DataFrame au schéma fixe des alertes ; testées avec une `SparkSession` locale, sans Kafka | Pas d'aller-retour JVM ↔ Python ; les tests portent sur le code qui tourne vraiment dans le job |
| J4 | Bornes : R2 = type sortant, montant > facteur × médiane (strict), montant ≥ plancher (inclusif), `nb_tx_30j` ≥ 5 ; profil absent ou médiane `null` : pas d'alerte, sans erreur. R3 = type sortant, montant > 500 000 (strict), heure de Dakar dans [01:00 ; 05:00[, **calculée sans dépendre du fuseau de la session** (`convert_timezone` depuis le fuseau de session) | Une seule référence pour les tests de bornes ; le cas `null` n'apparaît pas avec les profils d'amorçage, seul un test le couvre avant la phase 7 |
| J5 | `alert_id` = SHA-256 hexadécimal minuscule d'une chaîne canonique : `R2\|<transaction_id>`, `R3\|<transaction_id>`, et dès maintenant `R1\|<sender_id>\|<window_start en UTC, yyyy-MM-dd'T'HH:mm:ss.SSS'Z'>` | Identifiant déterministe : un rejeu retombe sur la même alerte. Fixer le format de R1 maintenant évite qu'un formatage implicite dépende du fuseau de session en phase 5 |
| J6 | Sévérités R2 = HAUTE, R3 = MOYENNE ; `details` contient les valeurs **et les seuils appliqués** | Une alerte reste compréhensible après un changement de configuration |
| J7 | Alertes en JDBC vers `alerts_staging` (`details` en texte), puis `INSERT … SELECT …, details::jsonb, clock_timestamp() … ON CONFLICT (alert_id) DO NOTHING` **dans la même transaction** que la fusion des transactions, la purge des transits et le bilan. **`clock_timestamp()` au lieu de `now()`** | `now()` est l'heure de **début** de la transaction : toutes les alertes d'un lot auraient la même heure, prise avant la fusion, et la latence serait sous-estimée. JDBC n'écrit pas de `jsonb`, d'où le transit en texte. `ON CONFLICT DO NOTHING` ne déclenche pas le trigger `BEFORE UPDATE` : un rejeu ne lève rien |
| J8 | `stream_batches` : `nb_alerts_by_rule`, `started_at` (début du lot côté driver), `processed_at` écrit avec `clock_timestamp()`, et **`nb_lus` compté** (`count()` du lot persisté) | Σ `nb_lus` = Σ `nb_sent` devient un contrôle indépendant de la classification ; `processed_at − started_at` = durée du lot, qui explique la latence |
| J9 | Le lot reste persisté pendant toutes les actions (comptage, transit, règles, alertes), puis libéré | Sinon chaque action relit l'intervalle d'offsets dans Kafka, et ce temps s'ajoute à la latence |
| J10 | Latence = `detected_at − ingested_at` de la transaction déclenchante : n, médiane, p95, max par règle, **sur un run en direct uniquement**, avec la durée des lots ; débit par défaut `--speed 60` | Sur un arriéré, les messages attendent dans Kafka : la latence n'y a pas de sens |
| J11 | `make stream-report` : alertes par règle ; **vrai positif = transaction de l'alerte présente dans la vérité terrain, quel que soit le type** ; faux positifs ; rappel par type attendu (`MONTANT_ANORMAL` → R2, `NUIT_GROS_MONTANT` → R3) ; latence sur une fenêtre de temps réel (`--run N`) | Contrôle de cohérence ; le rapport de détection complet viendra en phase 7 |
| J12 | Test du profil modifié, avec un témoin positif : client réservé hors population ; médiane 10 000 → alerte R2 sur un TRANSFER de 200 000 ; médiane passée à 100 000 en base → même montant, plus d'alerte, alors que la transaction est en base | Le témoin prouve que l'absence d'alerte vient du profil relu, pas d'un job arrêté. Ces messages ne sont pas dans `generator_runs` : le test passe en dernier, puis `make reset` |

Choix d'implémentation de la phase 4 :

| Choix | Pourquoi |
|---|---|
| La fusion (transactions, alertes, purge des deux transits, bilan) est une fonction à part, `merge_batch`, testée sur la vraie base dans une transaction annulée (rejeu → 0 insertion, `detected_at` et bilan inchangés) | Le chemin critique du rejeu est prouvé par un test, en plus du contrôle en conditions réelles |
| Le journal du job signale un lot rejoué (« bilan d'origine conservé ») | Un rejeu se voit sans aller lire la base |
| Pas de `CHECK (processed_at >= started_at)` sur `stream_batches` ; `make stream-report` signale les lots de durée négative | `started_at` vient de l'horloge du driver Spark, `processed_at` de celle de PostgreSQL : un écart entre les deux ne doit jamais faire échouer la fusion d'un lot |
| `tomli` aussi en dépendance de dev sur l'hôte | Le second passage de mypy (Python 3.10) doit résoudre `import tomli` |
