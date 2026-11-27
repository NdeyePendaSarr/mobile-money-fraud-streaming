"""Job Spark Structured Streaming SenPay, phase 3 : lecture Kafka → validation → PostgreSQL.

Lancé dans le conteneur ``spark-streaming`` (``make stream``) par ``spark-submit`` ;
les réglages de la session (``local[4]``, driver 2 Go, ``shuffle.partitions = 4``,
fuseau ``Africa/Dakar``, paquets épinglés) sont passés par ``docker-compose.yml``.

Deux sorties :

- ``--sink postgres`` (défaut) : ``foreachBatch`` → ``streaming.sinks.process_batch``,
  écriture idempotente, checkpoint dans ``SENPAY_CHECKPOINT_DIR`` ;
- ``--sink console`` : affiche les messages validés (mise au point), sans checkpoint
  durable et sans écrire en base.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.rules_config import RulesConfig, RulesConfigError, load_rules_config
from streaming.sinks import PgSettings, process_batch
from streaming.validation import validate

TOPIC = "transactions"
TRIGGER = "5 seconds"  # déclencheur (I7)
MAX_OFFSETS_PER_TRIGGER = 5_000  # borne la taille d'un micro-batch (I7)
EXPECTED_TIMEZONE = "Africa/Dakar"  # F9 : jamais le fuseau de la machine


def read_transactions(spark: SparkSession, bootstrap: str) -> DataFrame:
    """Flux brut du topic : texte du message et métadonnées Kafka conservées (F10)."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")  # I6 : un checkpoint vide relit tout
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
        .select(
            F.col("value").cast("string").alias("value"),
            # Horodatage Kafka (CreateTime, temps réel) : base de la latence (F10)
            F.col("timestamp").alias("ingested_at"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
    )


def _log_batch(settings: PgSettings, rules: RulesConfig) -> Callable[[DataFrame, int], None]:
    """Fonction ``foreachBatch`` : traite le lot et écrit une ligne de bilan sur stdout."""

    def handle(batch: DataFrame, batch_id: int) -> None:
        summary = process_batch(batch, batch_id, settings, rules)
        print(
            f"lot {summary.batch_id} : {summary.nb_lus} lus, {summary.nb_valides} valides, "
            f"{summary.nb_inseres} insérés, {summary.nb_invalides} invalides "
            f"{summary.nb_invalid_by_reason}, alertes {summary.nb_alerts_by_rule} "
            f"({summary.nb_alertes_inserees} insérées)"
            + (" (rejeu : lot déjà fusionné, bilan d'origine conservé)" if summary.replay else ""),
            flush=True,
        )

    return handle


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="streaming.job", description=__doc__.split("\n\n")[0])
    parser.add_argument("--sink", choices=("postgres", "console"), default="postgres")
    args = parser.parse_args(argv)

    # Seuils des règles lus avant Spark : une configuration invalide arrête le job (J1)
    rules_path = Path(os.environ.get("SENPAY_RULES_CONFIG", "config/rules.toml"))
    try:
        rules = load_rules_config(rules_path)
    except RulesConfigError as erreur:
        print(f"ÉCHEC : configuration des règles : {erreur}", flush=True)
        return 1

    bootstrap = os.environ.get("SENPAY_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    spark = SparkSession.builder.appName("senpay-streaming").getOrCreate()
    timezone = spark.conf.get("spark.sql.session.timeZone")
    if timezone != EXPECTED_TIMEZONE:
        print(f"ÉCHEC : spark.sql.session.timeZone = {timezone}, {EXPECTED_TIMEZONE} attendu")
        return 1

    stream = read_transactions(spark, bootstrap)
    if args.sink == "console":
        query = (
            validate(stream)
            .select("kafka_partition", "kafka_offset", "invalid_reason", "transaction_id", "type")
            .writeStream.format("console")
            .option("truncate", "false")
            .option("numRows", 10)
            .trigger(processingTime=TRIGGER)
            .start()
        )
    else:
        settings = PgSettings.from_env()
        checkpoint = os.environ.get("SENPAY_CHECKPOINT_DIR", "/opt/spark-checkpoints/transactions")
        query = (
            stream.writeStream.foreachBatch(_log_batch(settings, rules))
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=TRIGGER)
            .queryName("transactions_postgres")
            .start()
        )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
