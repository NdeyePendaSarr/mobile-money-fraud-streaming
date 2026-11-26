"""Validation Spark du schéma v1 (phase 3, I3) : un cas par motif, from_json, parité.

Le validateur de référence est ``producer/tests/schema_v1.py`` (Python pur) : chaque
cas est jugé par les deux, et le test de parité compare leurs verdicts sur les
30 530 messages du jour de référence.
"""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from producer.config import load_config
from producer.delivery import INVALID_REASONS, corrupt
from producer.events import Transaction
from producer.producer import plan_run
from producer.tests.schema_v1 import SchemaError
from producer.tests.schema_v1 import validate as validate_python
from streaming.schemas import CORRUPT_COLUMN, RAW_OPTIONS, RAW_SCHEMA
from streaming.validation import validate

# Jour de référence (phase 2) et heure de réception R = fin du jour simulé + 1 jour
SEED = 42
SIM_START = datetime(2026, 11, 23, tzinfo=UTC)
SIM_END = SIM_START + timedelta(days=1)
R = SIM_END + timedelta(days=1)

VALIDE: dict[str, object] = {
    "schema_version": 1,
    "transaction_id": "5e1439b7-193b-4a80-b5e5-c4c1e508166b",
    "event_time": "2026-11-23T00:01:20.814Z",
    "sender_id": "U-000112",
    "receiver_id": "U-005719",
    "type": "TRANSFER",
    "amount_xof": 1344,
    "region": "Diourbel",
    "channel": "APP",
    "agent_id": None,
}


def message(**changes: object) -> str:
    """Message valide modifié ; une valeur ``...`` retire le champ."""
    contenu = {k: v for k, v in {**VALIDE, **changes}.items() if v is not ...}
    return json.dumps(contenu, ensure_ascii=False, separators=(",", ":"))


def spark_reasons(spark: SparkSession, values: list[str]) -> list[str | None]:
    """Motifs Spark, dans l'ordre des messages (``ingested_at`` = R pour tous)."""
    df = spark.createDataFrame(
        [Row(idx=i, value=v, ingested_at=R) for i, v in enumerate(values)],
        "idx int, value string, ingested_at timestamp",
    )
    rows = validate(df).orderBy("idx").select("invalid_reason").collect()
    return [row["invalid_reason"] for row in rows]


def python_reason(value: str) -> str | None:
    """Motif du validateur de référence (``None`` = valide)."""
    try:
        validate_python(value, ingested_at=R)
    except SchemaError as refus:
        return refus.reason
    return None


# --- Un cas par motif -------------------------------------------------------

CAS: list[tuple[str, str, str | None]] = [
    ("valide", message(), None),
    ("JSON tronqué", message()[:25], "JSON_ILLISIBLE"),
    ("JSON sans accolade finale", message()[:-1], "JSON_ILLISIBLE"),
    ("chaîne vide", "", "JSON_ILLISIBLE"),
    ("tableau au lieu d'un objet", "[1, 2]", "JSON_ILLISIBLE"),
    ("champ manquant", message(region=...), "CHAMP_MANQUANT"),
    ("receiver_id absent (≠ null)", message(receiver_id=...), "CHAMP_MANQUANT"),
    ("champ inconnu", message(anomaly_type="RAFALE"), "CHAMP_INCONNU"),
    ("schema_version 2", message(schema_version=2), "SCHEMA_VERSION"),
    ("schema_version en texte", message(schema_version="1"), "SCHEMA_VERSION"),
    ("UUID en majuscules", message(transaction_id=str(VALIDE["transaction_id"]).upper()), "FORMAT"),
    ("event_time sans millisecondes", message(event_time="2026-11-23T00:01:20Z"), "FORMAT"),
    ("event_time futur", message(event_time="2026-11-25T00:05:00.001Z"), "EVENT_TIME_FUTUR"),
    ("event_time à R + 5 min (borne)", message(event_time="2026-11-25T00:05:00.000Z"), None),
    ("sender_id mal formé", message(sender_id="U-12"), "FORMAT"),
    ("type inconnu", message(type="REFUND"), "TYPE_INCONNU"),
    ("montant sous le minimum", message(amount_xof=99), "MONTANT_HORS_BORNES"),
    ("montant au-dessus du plafond", message(amount_xof=2_000_001), "MONTANT_HORS_BORNES"),
    ("montant en texte", message(amount_xof="1344"), "FORMAT"),
    ("montant décimal", message(amount_xof=1344.5), "FORMAT"),
    ("région inconnue", message(region="Touba"), "FORMAT"),
    ("TRANSFER par agent", message(channel="AGENT"), "CANAL_INCOHERENT"),
    ("CASH_IN par l'application", message(type="CASH_IN", receiver_id=None), "CANAL_INCOHERENT"),
    ("destinataire mal formé", message(receiver_id="M-0001"), "FORMAT"),
    ("transfert vers soi-même", message(receiver_id="U-000112"), "RECEVEUR_EGAL_EMETTEUR"),
    (
        "CASH_OUT sans agent",
        message(type="CASH_OUT", receiver_id=None, channel="AGENT"),
        "FORMAT",
    ),
    (
        "CASH_OUT valide",
        message(type="CASH_OUT", receiver_id=None, channel="AGENT", agent_id="A-0137"),
        None,
    ),
]


@pytest.mark.parametrize(("nom", "valeur", "attendu"), CAS, ids=[c[0] for c in CAS])
def test_un_cas_par_motif(spark: SparkSession, nom: str, valeur: str, attendu: str | None) -> None:
    (motif_spark,) = spark_reasons(spark, [valeur])
    assert motif_spark == attendu
    assert python_reason(valeur) == attendu  # même verdict que la référence


def test_les_8_motifs_du_generateur(spark: SparkSession) -> None:
    # Chaque motif de H15, appliqué par le générateur lui-même à des transactions variées
    plan = plan_run(load_config(), SEED, SIM_START, SIM_START + timedelta(hours=2))
    transactions: list[Transaction] = [e.transaction for s in plan.slices() for e in s][:50]
    valeurs: list[str] = []
    attendus: list[str] = []
    rng = random.Random("test:spark")
    for motif in INVALID_REASONS:
        for tx in transactions:
            valeur = corrupt(rng, tx, motif)
            if valeur is None:  # EVENT_TIME_FUTUR : construit à l'envoi, ici à R
                corps = tx.to_message()
                corps["event_time"] = (R + timedelta(minutes=6)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                valeur = json.dumps(corps, separators=(",", ":"))
            valeurs.append(valeur)
            attendus.append(motif)
    assert spark_reasons(spark, valeurs) == attendus


# --- from_json + columnNameOfCorruptRecord : comportement prouvé ------------


def test_corrupt_record_rempli_seulement_si_json_illisible(spark: SparkSession) -> None:
    valeurs = [message(), message()[:-1], message(amount_xof="abc"), message(amount_xof=True), ""]
    df = spark.createDataFrame([(v,) for v in valeurs], "value string")
    corrompus = [
        r[0]
        for r in df.select(
            F.from_json("value", RAW_SCHEMA, RAW_OPTIONS).getField(CORRUPT_COLUMN)
        ).collect()
    ]
    # JSON illisible : le texte d'origine est recopié dans la colonne
    assert corrompus[1] == message()[:-1]
    # Valide, ou mal typé : tous les champs sont lus en texte, rien n'est « corrompu »
    assert corrompus[0] is None and corrompus[2] is None and corrompus[3] is None
    # Chaîne vide : pas d'enregistrement corrompu → attrapée par json_object_keys (null)
    assert corrompus[4] is None


def test_un_schema_type_confondrait_type_et_json_illisible(spark: SparkSession) -> None:
    # Pourquoi RAW_SCHEMA lit tout en texte : avec un champ typé, un montant en texte
    # remplit aussi _corrupt_record, et serait compté comme JSON illisible
    typed = StructType(
        [
            StructField("amount_xof", LongType(), nullable=True),
            StructField(CORRUPT_COLUMN, StringType(), nullable=True),
        ]
    )
    df = spark.createDataFrame([(message(amount_xof="abc"),)], "value string")
    (row,) = df.select(F.from_json("value", typed, RAW_OPTIONS).alias("r")).collect()
    assert row["r"][CORRUPT_COLUMN] is not None


# --- Test de parité (I3) ----------------------------------------------------


def test_parite_sur_le_jour_de_reference(spark: SparkSession) -> None:
    """Sur le jour de référence en --dry-run, Spark et schema_v1 donnent le même verdict.

    Le motif EVENT_TIME_FUTUR dépend de l'heure de réception : le dry-run ne garde
    que le décalage. Le message est donc reconstruit comme à l'envoi, à l'heure R
    (fin du jour simulé + 1 jour), et la même heure R sert d'``ingested_at`` aux
    deux validateurs.
    """
    plan = plan_run(load_config(), SEED, SIM_START, SIM_END)
    valeurs = [e.render(R) for emissions in plan.slices() for e in emissions]
    assert len(valeurs) == 30_530

    references = [python_reason(v) for v in valeurs]
    obtenus = spark_reasons(spark, valeurs)

    ecarts = [(i, r, o) for i, (r, o) in enumerate(zip(references, obtenus, strict=True)) if r != o]
    assert ecarts == [], f"{len(ecarts)} écarts, premiers : {ecarts[:5]}"
    # Les 8 motifs du générateur sont présents, et seulement eux
    assert {r for r in references if r is not None} == set(INVALID_REASONS)
    assert sum(r is not None for r in references) == 135
