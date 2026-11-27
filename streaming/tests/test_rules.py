"""Règles R2 et R3 (phase 4) : cas de bornes de la phase 0 §6, ajouts R2-11, R2-12, R3-8.

Les règles sont des expressions Spark : elles sont testées avec une SparkSession
locale, sans Kafka (J3). R2-10 (profil modifié) est le test d'intégration J12 ;
R3-9 (événement très en retard) attend la phase 5.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from streaming.rules import ALERT_COLUMNS, alert_id_r1, apply_rules, rule_r2, rule_r3
from streaming.rules_config import RulesConfigError, load_rules_config, parse_rules_config

CFG = load_rules_config(Path("config/rules.toml"))
TX_SCHEMA = (
    "transaction_id string, event_time timestamp, sender_id string, type string, "
    "amount_xof long, ingested_at timestamp"
)
PROFILE_SCHEMA = "user_id string, median_amount_xof long, nb_tx_30j int"
INGESTED = datetime(2026, 11, 24, tzinfo=UTC)


@dataclass(frozen=True)
class Cas:
    nom: str
    type: str
    montant: int
    alerte: bool
    mediane: int | None = 10_000
    nb_tx_30j: int = 20
    profil: bool = True
    heure: str = "12:00:00.000"  # heure de Dakar (= UTC) de event_time


def tx_id(i: int) -> str:
    return f"00000000-0000-4000-8000-{i:012d}"


def event_time(heure: str) -> datetime:
    return datetime.fromisoformat(f"2026-11-23T{heure}+00:00")


def transactions(spark: SparkSession, cas: list[Cas]) -> DataFrame:
    """Une transaction par cas ; le client du cas i est U-00000i."""
    return spark.createDataFrame(
        [
            Row(
                transaction_id=tx_id(i),
                event_time=event_time(c.heure),
                sender_id=f"U-{i:06d}",
                type=c.type,
                amount_xof=c.montant,
                ingested_at=INGESTED,
            )
            for i, c in enumerate(cas)
        ],
        TX_SCHEMA,
    )


def profils(spark: SparkSession, cas: list[Cas]) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(user_id=f"U-{i:06d}", median_amount_xof=c.mediane, nb_tx_30j=c.nb_tx_30j)
            for i, c in enumerate(cas)
            if c.profil
        ],
        PROFILE_SCHEMA,
    )


@contextmanager
def fuseau_de_session(spark: SparkSession, fuseau: str) -> Iterator[None]:
    """Change le fuseau de la session le temps d'un bloc."""
    avant = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", fuseau)
    try:
        yield
    finally:
        spark.conf.set("spark.sql.session.timeZone", str(avant))


def ids_alertes(alertes: list[Row]) -> set[str]:
    return {str(a["transaction_id"]) for a in alertes}


# --- R2 : phase 0 §6 (R2-1 à R2-9) + ajouts R2-11, R2-12 --------------------

CAS_R2 = [
    Cas("R2-1 = 10 × médiane (strict)", "TRANSFER", 100_000, False),
    Cas("R2-2 > 10 × médiane", "TRANSFER", 100_001, True),
    Cas("R2-3 PAYMENT sortant", "PAYMENT", 150_000, True),
    Cas("R2-4 CASH_IN entrant", "CASH_IN", 500_000, False),
    Cas("R2-5 sous le plancher", "TRANSFER", 40_000, False, mediane=2_000),
    Cas("R2-6 plancher inclusif", "CASH_OUT", 50_000, True, mediane=4_000),
    Cas("R2-7 historique 4 < 5", "TRANSFER", 500_000, False, nb_tx_30j=4),
    Cas("R2-8 historique 5 = minimum", "TRANSFER", 500_000, True, nb_tx_30j=5),
    Cas("R2-9 client sans profil", "TRANSFER", 500_000, False, profil=False),
    Cas("R2-11 ratio > 10, sous le plancher", "TRANSFER", 49_999, False, mediane=4_000),
    Cas("R2-12 médiane NULL", "TRANSFER", 500_000, False, mediane=None),
]


@pytest.fixture(scope="module")
def alertes_r2(spark: SparkSession) -> list[Row]:
    return rule_r2(transactions(spark, CAS_R2), profils(spark, CAS_R2), CFG.r2).collect()


@pytest.mark.parametrize("index", range(len(CAS_R2)), ids=[c.nom for c in CAS_R2])
def test_r2_bornes(alertes_r2: list[Row], index: int) -> None:
    assert (tx_id(index) in ids_alertes(alertes_r2)) == CAS_R2[index].alerte


def test_r2_forme_et_details(alertes_r2: list[Row]) -> None:
    (alerte,) = [a for a in alertes_r2 if a["transaction_id"] == tx_id(1)]  # R2-2
    assert alerte["rule_id"] == "R2" and alerte["severity"] == "HAUTE"
    assert alerte["window_start"] is None and alerte["window_end"] is None
    assert alerte["event_time"] is not None
    # Valeurs et seuils appliqués (J6) : l'alerte reste lisible après un changement de config
    assert json.loads(alerte["details"]) == {
        "montant_xof": 100_001,
        "mediane_xof": 10_000,
        "ratio": 10.0001,
        "nb_tx_30j": 20,
        "facteur": 10.0,
        "plancher_xof": 50_000,
        "historique_min": 5,
    }


# --- R3 : phase 0 §6 (R3-1 à R3-7) + R3-8 réécrit ----------------------------

CAS_R3 = [
    Cas("R3-1 = 500 000 (strict)", "CASH_OUT", 500_000, False, heure="02:00:00.000"),
    Cas("R3-2 05:00 exclu", "TRANSFER", 500_001, False, heure="05:00:00.000"),
    Cas("R3-3 juste avant 05:00", "TRANSFER", 500_001, True, heure="04:59:59.999"),
    Cas("R3-4 01:00 inclus", "PAYMENT", 500_001, True, heure="01:00:00.000"),
    Cas("R3-5 juste avant 01:00", "TRANSFER", 500_001, False, heure="00:59:59.999"),
    Cas("R3-6 CASH_IN entrant", "CASH_IN", 800_000, False, heure="03:00:00.000"),
    Cas("R3-7 hors de la nuit", "CASH_OUT", 2_000_000, False, heure="23:30:00.000"),
]


@pytest.fixture(scope="module")
def alertes_r3(spark: SparkSession) -> list[Row]:
    return rule_r3(transactions(spark, CAS_R3), CFG.r3).collect()


@pytest.mark.parametrize("index", range(len(CAS_R3)), ids=[c.nom for c in CAS_R3])
def test_r3_bornes(alertes_r3: list[Row], index: int) -> None:
    assert (tx_id(index) in ids_alertes(alertes_r3)) == CAS_R3[index].alerte


def test_r3_forme_et_details(alertes_r3: list[Row]) -> None:
    (alerte,) = [a for a in alertes_r3 if a["transaction_id"] == tx_id(2)]  # R3-3
    assert alerte["rule_id"] == "R3" and alerte["severity"] == "MOYENNE"
    assert alerte["window_start"] is None and alerte["window_end"] is None
    assert json.loads(alerte["details"]) == {
        "montant_xof": 500_001,
        "heure_locale": "04:59:59.999",
        "seuil_xof": 500_000,
        "heure_debut": 1,
        "heure_fin": 5,
        "fuseau": "Africa/Dakar",
    }


@pytest.mark.parametrize("fuseau", ["America/New_York", "Asia/Tokyo", "UTC"])
def test_r3_8_independant_du_fuseau_de_session(
    spark: SparkSession, alertes_r3: list[Row], fuseau: str
) -> None:
    # R3-8 réécrit (§2) : TimestampType est un instant, il n'a pas de version « naïve ».
    # Équivalent de F9 : le même lot, sous un autre fuseau de session, donne les mêmes
    # alertes, avec la même heure locale de Dakar dans details.
    with fuseau_de_session(spark, fuseau):
        autres = rule_r3(transactions(spark, CAS_R3), CFG.r3).collect()
    assert sorted(a.asDict().items() for a in autres) == sorted(
        a.asDict().items() for a in alertes_r3
    )


# --- Croisement R2 × R3, schéma des alertes ----------------------------------


def test_croisement_r2_r3_deux_alertes(spark: SparkSession) -> None:
    # TRANSFER de 600 000 à 02:30, médiane 10 000 : R2 et R3, deux alert_id distincts
    cas = [Cas("R2 × R3", "TRANSFER", 600_000, True, heure="02:30:00.000")]
    alertes = apply_rules(transactions(spark, cas), profils(spark, cas), CFG.r2, CFG.r3).collect()
    assert sorted(a["rule_id"] for a in alertes) == ["R2", "R3"]
    assert len({a["alert_id"] for a in alertes}) == 2
    assert {a["transaction_id"] for a in alertes} == {tx_id(0)}


def test_schema_fixe_des_alertes(spark: SparkSession) -> None:
    cas = [Cas("aucune alerte", "TRANSFER", 1_000, False)]
    alertes = apply_rules(transactions(spark, cas), profils(spark, cas), CFG.r2, CFG.r3)
    assert tuple(alertes.columns) == ALERT_COLUMNS
    assert alertes.count() == 0


# --- alert_id (J5) -------------------------------------------------------------


def sha256(texte: str) -> str:
    return hashlib.sha256(texte.encode("utf-8")).hexdigest()


def test_alert_id_r2_r3_canonique(spark: SparkSession) -> None:
    cas = [Cas("R2 × R3", "TRANSFER", 600_000, True, heure="02:30:00.000")]
    alertes = apply_rules(transactions(spark, cas), profils(spark, cas), CFG.r2, CFG.r3).collect()
    attendus = {r: sha256(f"{r}|{tx_id(0)}") for r in ("R2", "R3")}
    for alerte in alertes:
        assert alerte["alert_id"] == attendus[alerte["rule_id"]]
        assert len(alerte["alert_id"]) == 64 and alerte["alert_id"] == alerte["alert_id"].lower()


@pytest.mark.parametrize("fuseau", ["Africa/Dakar", "America/New_York", "Asia/Tokyo"])
def test_alert_id_r1_en_utc_quel_que_soit_le_fuseau(spark: SparkSession, fuseau: str) -> None:
    # Chaîne R1 fixée dès la phase 4 : window_start formaté explicitement en UTC
    debut = datetime(2026, 11, 23, 12, 0, 30, tzinfo=UTC)
    with fuseau_de_session(spark, fuseau):
        df = spark.createDataFrame(
            [Row(sender_id="U-000042", window_start=debut)],
            "sender_id string, window_start timestamp",
        )
        (row,) = df.select(alert_id_r1(df["sender_id"], df["window_start"]).alias("id")).collect()
    assert row["id"] == sha256("R1|U-000042|2026-11-23T12:00:30.000Z")


# --- Configuration (J1) --------------------------------------------------------

VALIDE: dict[str, Any] = {
    "r2": {"factor": 10.0, "floor_xof": 50_000, "min_history": 5},
    "r3": {
        "amount_threshold_xof": 500_000,
        "hour_start": 1,
        "hour_end": 5,
        "timezone": "Africa/Dakar",
    },
}


def test_config_du_depot() -> None:
    assert CFG.r2.factor == 10.0 and CFG.r2.floor_xof == 50_000 and CFG.r2.min_history == 5
    assert (CFG.r3.amount_threshold_xof, CFG.r3.hour_start, CFG.r3.hour_end) == (500_000, 1, 5)


@pytest.mark.parametrize(
    ("section", "cle", "valeur"),
    [
        ("r2", "floor_xof", None),  # clé manquante
        ("r2", "factor", "dix"),
        ("r2", "factor", 1),
        ("r2", "min_history", -1),
        ("r3", "hour_end", 1),  # fin ≤ début
        ("r3", "hour_start", 24),
        ("r3", "timezone", "Mars/Olympus"),
        ("r3", "inconnue", 3),  # clé inconnue
    ],
)
def test_config_invalide_arrete(section: str, cle: str, valeur: object) -> None:
    data = {s: dict(t) for s, t in VALIDE.items()}
    if valeur is None:
        del data[section][cle]
    else:
        data[section][cle] = valeur
    with pytest.raises(RulesConfigError):
        parse_rules_config(data)


def test_config_fichier_absent(tmp_path: Path) -> None:
    with pytest.raises(RulesConfigError):
        load_rules_config(tmp_path / "absent.toml")
