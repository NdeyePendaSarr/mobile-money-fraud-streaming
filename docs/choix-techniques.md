# Choix techniques

Choix d'implémentation du socle (phase 1) et du générateur (phase 2, décisions H1 à H22), en
complément du cadrage de la phase 0 (décisions F1 à F22).

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
