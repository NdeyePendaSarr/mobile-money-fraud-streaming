# Choix techniques

Choix d'implémentation du socle, en complément du cadrage de la phase 0 (décisions F1 à F22).

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
