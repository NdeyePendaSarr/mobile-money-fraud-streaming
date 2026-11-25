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

## Figées pour les phases suivantes (non installées en phase 1)

| Composant | Version | Coordonnées | Remarque |
|---|---|---|---|
| Apache Spark / PySpark | **4.2.1** (27/10/2026) | `pyspark==4.2.1` | Dernier correctif de la branche 4.2. La 4.3.0 (30/10/2026) est écartée : version .0 trop récente |
| Connecteur Kafka de Spark | **4.2.1** | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1` | Même version que Spark, Scala 2.13 ; embarque `kafka-clients` 3.9.2, compatible avec un broker 4.x |
| Pilote JDBC PostgreSQL | **42.7.14** (07/10/2026) | `org.postgresql:postgresql:42.7.14` | Écriture `foreachBatch` → PostgreSQL. Correctif précédant la 42.7.15 (23/11/2026, trop récente) : **à revérifier en phase 3** |
| Java (pour Spark) | 17 ou 21 | — | Spark 4.2 est compilé pour Java 17 |

Paquets Spark à passer au lancement du job (phase 3) :

```
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1,org.postgresql:postgresql:42.7.14
```

## Règle de mise à jour

Une montée de version se fait par un commit dédié qui modifie ce fichier et le tag (ou `uv.lock`)
en même temps, après `make reset`, `make up` et `make check` verts.
