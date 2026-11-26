# Versions figées

Versions épinglées dès la phase 1 (PRD §8.4), vérifiées le 25/11/2026 sur les sources officielles
(Docker Hub, Maven Central, PyPI). Les images Docker sont épinglées par tag exact dans
`docker-compose.yml` ; les paquets Python par `uv.lock`.

## En place (phase 1)

| Composant | Version | Où | Remarque |
|---|---|---|---|
| Apache Kafka | **4.3.2** (image `apache/kafka:4.3.2`, 20/10/2026) | `docker-compose.yml` | KRaft, 1 nœud broker + contrôleur ; Java 21 dans l'image |
| PostgreSQL | **17.12** (image `postgres:17.12`, 24/11/2026) | `docker-compose.yml` | Correctif publié peu après la 17.11 (06/11) |
| Python | **3.12** (`.python-version`) | `pyproject.toml` | `requires-python = ">=3.12,<3.13"` |
| confluent-kafka | 2.16.2 | `uv.lock` | Admin Kafka de `scripts/check_infra.py`, puis producteur (phase 2) |
| psycopg | 3.3.9 | `uv.lock` | Accès PostgreSQL |

## Spark (phase 3, revérifié le 26/11/2026)

| Composant | Version | Coordonnées | Remarque |
|---|---|---|---|
| Image du job | **`apache/spark:4.2.1-scala2.13-java17-python3-ubuntu`** (publiée le 02/11/2026) | `streaming/Dockerfile` | Ubuntu 22.04, OpenJDK 17.0.20, **Python 3.10.12** ; pyspark fourni par `$SPARK_HOME/python`. On y ajoute seulement psycopg 3.3.9 (même version que `uv.lock`) |
| Apache Spark / PySpark | **4.2.1** | image ; `pyspark==4.2.1` en dépendance de dev (tests sur l'hôte) | Dernier correctif de la branche 4.2 (supportée jusqu'au 11/01/2028). La 4.3.0 (fin octobre 2026) est écartée : version .0 trop récente |
| Connecteur Kafka de Spark | **4.2.1** | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1` | Même version que Spark (obligatoire), Scala 2.13 ; embarque `kafka-clients` 3.9.2, compatible avec un broker 4.x |
| Pilote JDBC PostgreSQL | **42.7.14** (07/10/2026) | `org.postgresql:postgresql:42.7.14` | Écriture `foreachBatch` → `transactions_staging`. Revérifié en phase 3 : conservé, la 42.7.15 (23/11/2026) n'a que 3 jours |
| Java sur l'hôte (tests) | 17 | `openjdk-17-jdk-headless` | Même version majeure que l'image |

Les paquets sont passés par `spark.jars.packages` (`docker-compose.yml`) et résolus par Ivy
au premier démarrage, avec leurs dépendances transitives ; le cache Ivy vit dans un volume
nommé. Le code du job (`streaming/*.py`) est vérifié en Python 3.10 (ruff, mypy, import réel
dans l'image par `make check-image`) ; ses tests tournent sur l'hôte en 3.12.

## Règle de mise à jour

Une montée de version se fait par un commit dédié qui modifie ce fichier et le tag (ou `uv.lock`)
en même temps, après `make reset`, `make up` et `make check` verts.
