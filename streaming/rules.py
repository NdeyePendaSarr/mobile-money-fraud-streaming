"""Règles de détection R2 et R3 en expressions Spark natives (phase 4, J3 à J6).

Fonctions pures : un DataFrame de transactions valides (et, pour R2, un instantané
des profils) en entrée, un DataFrame d'alertes au **schéma fixe** en sortie
(``ALERT_COLUMNS``). Aucune UDF Python, comme la validation (I3) : les tests
portent sur le code qui tourne vraiment dans le job, avec une SparkSession locale.

Bornes (cadrage phase 0 §3, J4) :

- R2 : type sortant ; montant > facteur × médiane (strict) ; montant ≥ plancher
  (inclusif) ; ``nb_tx_30j`` ≥ historique minimal. Profil absent ou médiane
  ``null`` : pas d'alerte, sans erreur (``null`` en SQL n'est jamais vrai).
- R3 : type sortant ; montant > seuil (strict) ; heure **locale** du fuseau métier
  dans [début ; fin[. L'heure locale ne dépend pas du fuseau de la session Spark :
  ``convert_timezone`` sans fuseau source part du fuseau de la session, quel qu'il
  soit, et rend l'heure murale du fuseau métier (F9).

``alert_id`` = SHA-256 hexadécimal minuscule (64 caractères) d'une chaîne canonique
(J5) : ``R2|<transaction_id>``, ``R3|<transaction_id>`` et, pour la phase 5,
``R1|<sender_id>|<window_start en UTC, yyyy-MM-dd'T'HH:mm:ss.SSS'Z'>``.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from streaming.rules_config import R2Config, R3Config

OUTGOING_TYPES = ("TRANSFER", "PAYMENT", "CASH_OUT")

# Sévérités de la phase 0 §3 (J6)
SEVERITY_R2 = "HAUTE"
SEVERITY_R3 = "MOYENNE"

# Schéma fixe des alertes, dans l'ordre de la table alerts (details en texte JSON)
ALERT_COLUMNS: tuple[str, ...] = (
    "alert_id",
    "rule_id",
    "transaction_id",
    "sender_id",
    "severity",
    "event_time",
    "window_start",
    "window_end",
    "ingested_at",
    "details",
)

# Format explicite de window_start dans l'alert_id de R1 (J5)
R1_WINDOW_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"


# --- Identifiants d'alerte (J5) ---------------------------------------------


def alert_id_for_transaction(rule_id: str, transaction_id: Column) -> Column:
    """``sha2('R2|' + transaction_id, 256)`` (ou R3) : une alerte par règle et transaction."""
    return F.sha2(F.concat(F.lit(f"{rule_id}|"), transaction_id), 256)


def utc_text(instant: Column, pattern: str = R1_WINDOW_FORMAT) -> Column:
    """Texte UTC d'un instant, indépendant du fuseau de la session Spark."""
    return F.date_format(F.convert_timezone(None, F.lit("UTC"), instant), pattern)


def alert_id_r1(sender_id: Column, window_start: Column) -> Column:
    """``sha2('R1|' + sender_id + '|' + window_start UTC, 256)`` (fixé dès la phase 4)."""
    return F.sha2(
        F.concat(F.lit("R1|"), sender_id, F.lit("|"), utc_text(window_start)),
        256,
    )


def local_wall_clock(instant: Column, timezone: str) -> Column:
    """Heure murale (``TIMESTAMP_NTZ``) de ``instant`` dans ``timezone``.

    ``convert_timezone(None, …)`` : la source est le fuseau de la session, donc le
    résultat ne dépend pas de ce fuseau.
    """
    return F.convert_timezone(None, F.lit(timezone), instant)


def _alerts(
    df: DataFrame, rule_id: str, severity: str, details: Column, condition: Column
) -> DataFrame:
    """Met les transactions retenues au schéma fixe des alertes (forme R2/R3, J6)."""
    sans_fenetre = F.lit(None).cast("timestamp")
    return df.where(condition).select(
        alert_id_for_transaction(rule_id, F.col("transaction_id")).alias("alert_id"),
        F.lit(rule_id).alias("rule_id"),
        F.col("transaction_id"),
        F.col("sender_id"),
        F.lit(severity).alias("severity"),
        F.col("event_time"),
        sans_fenetre.alias("window_start"),
        sans_fenetre.alias("window_end"),
        F.col("ingested_at"),
        F.to_json(details).alias("details"),
    )


# --- Règles -----------------------------------------------------------------


def rule_r2(tx: DataFrame, profiles: DataFrame, cfg: R2Config) -> DataFrame:
    """R2 – montant anormal, sur un instantané des profils (jointure lot ↔ profils, J2).

    ``profiles`` : ``user_id``, ``median_amount_xof``, ``nb_tx_30j``. Jointure interne :
    un client sans profil ne produit rien (R2-9).
    """
    p = profiles.select(
        F.col("user_id").alias("_user_id"),
        F.col("median_amount_xof").alias("_median"),
        F.col("nb_tx_30j").alias("_nb_tx_30j"),
    )
    joined = tx.join(F.broadcast(p), tx["sender_id"] == p["_user_id"], "inner")
    montant, mediane = F.col("amount_xof"), F.col("_median")
    condition = (
        F.col("type").isin(*OUTGOING_TYPES)
        & (montant > F.lit(cfg.factor) * mediane)  # médiane null → null → non retenu
        & (montant >= F.lit(cfg.floor_xof))
        & (F.col("_nb_tx_30j") >= F.lit(cfg.min_history))
    )
    details = F.struct(
        montant.alias("montant_xof"),
        mediane.alias("mediane_xof"),
        F.round(montant / mediane, 4).alias("ratio"),
        F.col("_nb_tx_30j").alias("nb_tx_30j"),
        F.lit(cfg.factor).alias("facteur"),
        F.lit(cfg.floor_xof).alias("plancher_xof"),
        F.lit(cfg.min_history).alias("historique_min"),
    )
    return _alerts(joined, "R2", SEVERITY_R2, details, condition)


def rule_r3(tx: DataFrame, cfg: R3Config) -> DataFrame:
    """R3 – nuit et gros montant, heure locale calculée sans le fuseau de la session (F9)."""
    locale = local_wall_clock(F.col("event_time"), cfg.timezone)
    heure = F.hour(locale)
    condition = (
        F.col("type").isin(*OUTGOING_TYPES)
        & (F.col("amount_xof") > F.lit(cfg.amount_threshold_xof))
        & (heure >= F.lit(cfg.hour_start))
        & (heure < F.lit(cfg.hour_end))
    )
    details = F.struct(
        F.col("amount_xof").alias("montant_xof"),
        F.date_format(locale, "HH:mm:ss.SSS").alias("heure_locale"),
        F.lit(cfg.amount_threshold_xof).alias("seuil_xof"),
        F.lit(cfg.hour_start).alias("heure_debut"),
        F.lit(cfg.hour_end).alias("heure_fin"),
        F.lit(cfg.timezone).alias("fuseau"),
    )
    return _alerts(tx, "R3", SEVERITY_R3, details, condition)


def apply_rules(tx: DataFrame, profiles: DataFrame, r2: R2Config, r3: R3Config) -> DataFrame:
    """Alertes R2 et R3 du lot ; une transaction peut déclencher les deux (deux alert_id)."""
    return rule_r2(tx, profiles, r2).unionByName(rule_r3(tx, r3))
