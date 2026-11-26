"""Validation du schéma v1 en expressions Spark natives (phase 3, I3).

Aucune UDF Python : une UDF ferait passer chaque ligne de la JVM vers Python et
retour, ce qui est lent et coûteux en mémoire. Tout est exprimé avec les
fonctions de ``pyspark.sql.functions``, évaluées dans la JVM.

``validate`` ajoute une colonne ``invalid_reason`` : ``null`` si le message est
valide, sinon **un seul** motif, le premier rencontré dans le même ordre de
priorité que le validateur de référence ``producer/tests/schema_v1.py`` :

1. ``JSON_ILLISIBLE``        JSON illisible ou pas un objet
2. ``CHAMP_MANQUANT``        un des 10 champs absent
3. ``CHAMP_INCONNU``         un champ hors schéma
4. ``SCHEMA_VERSION``        ``schema_version`` ≠ 1
5. ``FORMAT``                ``transaction_id`` (UUID v4), puis ``event_time``
6. ``EVENT_TIME_FUTUR``      ``event_time`` > ``ingested_at`` + 5 min
7. ``FORMAT``                ``sender_id``
8. ``TYPE_INCONNU``          type hors des 4 types
9. ``FORMAT`` / ``MONTANT_HORS_BORNES``  montant non entier / hors [100 ; 2 000 000]
10. ``FORMAT``               région
11. ``CANAL_INCOHERENT``, ``FORMAT``, ``RECEVEUR_EGAL_EMETTEUR``  matrice type × canal

Le générateur ne produit que 8 de ces motifs (H15) ; ``CHAMP_INCONNU`` et
``FORMAT`` sont gardés pour que tout message hors schéma soit écarté **avant**
l'écriture : sinon un CHECK de ``transactions`` ferait échouer tout le micro-batch.

Toutes les conditions sont écrites « valeur conforme » puis inversées avec
``_fails`` : une comparaison avec ``null`` donne ``null`` en SQL, et ``null`` doit
compter comme un échec, jamais comme un succès.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

from streaming.schemas import (
    AGENT,
    CLIENT,
    CORRUPT_COLUMN,
    EVENT_TIME,
    EVENT_TIME_PATTERN,
    FIELDS,
    MARCHAND,
    RAW_OPTIONS,
    RAW_SCHEMA,
    REGIONS,
    TYPES,
    UUID_V4,
)

# Colonnes du résultat, dans l'ordre de la table transactions
OUTPUT_COLUMNS: tuple[str, ...] = (
    "transaction_id",
    "schema_version",
    "event_time",
    "sender_id",
    "receiver_id",
    "type",
    "amount_xof",
    "region",
    "channel",
    "agent_id",
)


def _fails(conforme: Column) -> Column:
    """Vrai si la condition de conformité est fausse **ou** indéterminée (null)."""
    return ~F.coalesce(conforme, F.lit(False))


def _typed(value: Column, name: str, data_type: IntegerType | LongType) -> Column:
    """Lit un seul champ avec son vrai type JSON ; ``null`` si le type ne convient pas.

    Un champ par appel : une erreur de type sur un champ ne peut pas annuler l'autre.
    """
    schema = StructType([StructField(name, data_type, nullable=True)])
    return F.from_json(value, schema).getField(name)


def invalid_reason(value: Column, ingested_at: Column) -> Column:
    """Expression du motif d'invalidité (``null`` = message valide).

    ``value`` : message Kafka en texte ; ``ingested_at`` : horodatage Kafka.
    """
    raw = F.from_json(value, RAW_SCHEMA, RAW_OPTIONS)

    def field(name: str) -> Column:
        return raw.getField(name)

    keys = F.json_object_keys(value)  # null si JSON illisible ou pas un objet
    attendus = F.array(*[F.lit(name) for name in FIELDS])
    schema_version = _typed(value, "schema_version", IntegerType())
    amount = _typed(value, "amount_xof", LongType())
    event_time = F.try_to_timestamp(field("event_time"), F.lit(EVENT_TIME_PATTERN))
    tx_type, channel = field("type"), field("channel")
    sender, receiver, agent = field("sender_id"), field("receiver_id"), field("agent_id")
    sortant_app = tx_type.isin("TRANSFER", "PAYMENT")
    # Destinataire : un client pour un TRANSFER, un marchand pour un PAYMENT
    receveur_conforme = F.when(tx_type == "TRANSFER", receiver.rlike(CLIENT)).otherwise(
        receiver.rlike(MARCHAND)
    )

    # Matrice type × canal, dans l'ordre de schema_v1 (§2.2 du cadrage phase 0)
    motif_app = (
        F.when(_fails(channel.isin("APP", "USSD") & agent.isNull()), F.lit("CANAL_INCOHERENT"))
        .when(_fails(receveur_conforme), F.lit("FORMAT"))
        .when(receiver == sender, F.lit("RECEVEUR_EGAL_EMETTEUR"))
    )
    motif_agent = (
        F.when(_fails(channel == "AGENT"), F.lit("CANAL_INCOHERENT"))
        .when(_fails(agent.rlike(AGENT)), F.lit("FORMAT"))
        .when(receiver.isNotNull(), F.lit("FORMAT"))
    )

    return (
        F.when(
            value.isNull() | field(CORRUPT_COLUMN).isNotNull() | keys.isNull(),
            F.lit("JSON_ILLISIBLE"),
        )
        .when(F.size(F.array_except(attendus, keys)) > 0, F.lit("CHAMP_MANQUANT"))
        .when(F.size(F.array_except(keys, attendus)) > 0, F.lit("CHAMP_INCONNU"))
        .when(_fails(schema_version == 1), F.lit("SCHEMA_VERSION"))
        .when(_fails(field("transaction_id").rlike(UUID_V4)), F.lit("FORMAT"))
        .when(_fails(field("event_time").rlike(EVENT_TIME)), F.lit("FORMAT"))
        # Format conforme mais date impossible (ex. 2026-13-45) : FORMAT aussi
        .when(event_time.isNull(), F.lit("FORMAT"))
        .when(event_time > ingested_at + F.expr("INTERVAL 5 MINUTES"), F.lit("EVENT_TIME_FUTUR"))
        .when(_fails(sender.rlike(CLIENT)), F.lit("FORMAT"))
        .when(_fails(tx_type.isin(*TYPES)), F.lit("TYPE_INCONNU"))
        .when(amount.isNull(), F.lit("FORMAT"))
        .when(_fails(amount.between(100, 2_000_000)), F.lit("MONTANT_HORS_BORNES"))
        .when(_fails(field("region").isin(*REGIONS)), F.lit("FORMAT"))
        .otherwise(F.when(sortant_app, motif_app).otherwise(motif_agent))
    )


def validate(df: DataFrame) -> DataFrame:
    """Ajoute ``invalid_reason`` et les champs typés à un DataFrame ``value``, ``ingested_at``.

    Les autres colonnes d'entrée (``kafka_partition``, ``kafka_offset``…) sont gardées.
    Pour un message invalide, les champs typés peuvent être ``null`` : seule la
    colonne ``invalid_reason`` fait foi.
    """
    raw = F.from_json(F.col("value"), RAW_SCHEMA, RAW_OPTIONS)
    return df.select(
        "*",
        invalid_reason(F.col("value"), F.col("ingested_at")).alias("invalid_reason"),
        raw.getField("transaction_id").alias("transaction_id"),
        _typed(F.col("value"), "schema_version", IntegerType()).alias("schema_version"),
        F.try_to_timestamp(raw.getField("event_time"), F.lit(EVENT_TIME_PATTERN)).alias(
            "event_time"
        ),
        raw.getField("sender_id").alias("sender_id"),
        raw.getField("receiver_id").alias("receiver_id"),
        raw.getField("type").alias("type"),
        _typed(F.col("value"), "amount_xof", LongType()).alias("amount_xof"),
        raw.getField("region").alias("region"),
        raw.getField("channel").alias("channel"),
        raw.getField("agent_id").alias("agent_id"),
    )
