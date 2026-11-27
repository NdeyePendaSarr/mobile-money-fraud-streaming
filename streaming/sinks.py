"""Écriture idempotente d'un micro-batch dans PostgreSQL (phases 3 et 4 : I4, I5, J7, J8).

``foreachBatch`` est *at-least-once* : après un arrêt brutal, Spark rejoue le
dernier micro-batch (mêmes offsets, même ``batch_id``). Une écriture JDBC
``append`` directe dans ``transactions`` violerait alors la clé primaire, et le
job planterait à chaque relance. D'où, pour chaque micro-batch :

1. suppression des restes éventuels du lot dans ``transactions_staging`` **et**
   ``alerts_staging`` (tentative précédente interrompue après l'écriture JDBC) ;
2. règles R2 et R3 sur les messages valides, avec les profils **relus par JDBC**
   à chaque lot (J2), puis écriture JDBC des valides et des alertes en transit ;
3. depuis le driver, **une seule transaction** psycopg (``merge_batch``) :
   fusion des transactions et des alertes (``ON CONFLICT … DO NOTHING``, J7),
   suppression des deux transits, bilan dans ``stream_batches``
   (``ON CONFLICT (batch_id) DO NOTHING``).

Comme fusions et bilan sont validés ensemble, un bilan présent prouve que les
transactions **et les alertes** du lot sont en base. Un rejeu après l'étape 3
n'insère rien et laisse le bilan d'origine intact ; un rejeu avant l'étape 3
refait tout. Dans les deux cas : ni perte ni doublon. ``detected_at`` et
``processed_at`` sont écrits avec ``clock_timestamp()`` (heure réelle de
l'écriture) et non ``now()`` (heure de **début** de la transaction).
Les invalides sont seulement comptés (``dead_letter`` : phase 6).
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.rules import ALERT_COLUMNS, apply_rules
from streaming.rules_config import RulesConfig
from streaming.validation import OUTPUT_COLUMNS, validate

STAGING_TABLE = "transactions_staging"
ALERTS_STAGING_TABLE = "alerts_staging"

# Instantané des profils lu à chaque lot (J2) : jamais mis en cache
PROFILES_QUERY = "select user_id, median_amount_xof, nb_tx_30j from user_profiles"

# Colonnes écrites dans la table de transit (dans cet ordre)
STAGING_COLUMNS: tuple[str, ...] = (
    "batch_id",
    *OUTPUT_COLUMNS,
    "ingested_at",
    "kafka_partition",
    "kafka_offset",
)

# Fusion idempotente : le typage (uuid) se fait ici, pas dans Spark
MERGE_SQL = """
insert into transactions (transaction_id, schema_version, event_time, sender_id,
                          receiver_id, type, amount_xof, region, channel, agent_id,
                          ingested_at, kafka_partition, kafka_offset)
select transaction_id::uuid, schema_version, event_time, sender_id,
       receiver_id, type, amount_xof, region, channel, agent_id,
       ingested_at, kafka_partition, kafka_offset
from transactions_staging
where batch_id = %(batch_id)s
order by kafka_partition, kafka_offset
on conflict (transaction_id) do nothing
"""

# Fusion des alertes (J7) : details texte → jsonb ; detected_at écrit explicitement
# avec clock_timestamp() (le DEFAULT now() du schéma reste en filet). ON CONFLICT
# DO NOTHING ne déclenche pas le trigger BEFORE UPDATE : un rejeu ne lève rien.
MERGE_ALERTS_SQL = """
insert into alerts (alert_id, rule_id, transaction_id, sender_id, severity, event_time,
                    window_start, window_end, ingested_at, details, detected_at)
select alert_id, rule_id, transaction_id::uuid, sender_id, severity, event_time,
       window_start, window_end, ingested_at, details::jsonb, clock_timestamp()
from alerts_staging
where batch_id = %(batch_id)s
order by ingested_at, alert_id
on conflict (alert_id) do nothing
"""

# Bilan du lot, écrit une seule fois : au rejeu d'un lot déjà fusionné, la ligne
# existe déjà (même transaction que les fusions) et garde ses comptes d'origine
INSERT_BATCH_SQL = """
insert into stream_batches (batch_id, nb_lus, nb_valides, nb_inseres, nb_invalides,
                            nb_invalid_by_reason, nb_alerts_by_rule, started_at,
                            processed_at)
values (%(batch_id)s, %(nb_lus)s, %(nb_valides)s, %(nb_inseres)s, %(nb_invalides)s,
        %(par_motif)s::jsonb, %(par_regle)s::jsonb, %(started_at)s, clock_timestamp())
on conflict (batch_id) do nothing
"""


@dataclass(frozen=True)
class PgSettings:
    """Connexion à la base du projet, lue dans les variables SENPAY_PG_*."""

    host: str
    port: int
    user: str
    password: str
    dbname: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> PgSettings:
        manquantes = [
            name
            for name in (
                "SENPAY_PG_HOST",
                "SENPAY_PG_PORT",
                "SENPAY_PG_USER",
                "SENPAY_PG_PASSWORD",
                "SENPAY_PG_DB",
            )
            if not env.get(name)
        ]
        if manquantes:
            raise RuntimeError(f"variables manquantes : {', '.join(manquantes)}")
        return cls(
            host=env["SENPAY_PG_HOST"],
            port=int(env["SENPAY_PG_PORT"]),
            user=env["SENPAY_PG_USER"],
            password=env["SENPAY_PG_PASSWORD"],
            dbname=env["SENPAY_PG_DB"],
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"

    def jdbc_properties(self) -> dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
            "batchsize": "5000",
        }

    def connect(self) -> psycopg.Connection[tuple[object, ...]]:
        return psycopg.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
            connect_timeout=10,
        )


@dataclass(frozen=True)
class BatchSummary:
    """Bilan d'une tentative de micro-batch (écrit dans ``stream_batches`` sauf au rejeu)."""

    batch_id: int
    nb_lus: int  # compté sur le lot (J8), pas déduit des autres comptes
    nb_valides: int
    nb_inseres: int
    nb_invalid_by_reason: dict[str, int]
    nb_alerts_by_rule: dict[str, int] = field(default_factory=dict)  # alertes produites
    nb_alertes_inserees: int = 0  # 0 au rejeu d'un lot déjà fusionné
    replay: bool = False  # vrai si le lot avait déjà été fusionné (bilan d'origine conservé)

    @property
    def nb_invalides(self) -> int:
        return sum(self.nb_invalid_by_reason.values())


@dataclass(frozen=True)
class MergeResult:
    """Ce que la transaction de fusion a réellement écrit."""

    nb_inseres: int
    nb_alertes_inserees: int
    bilan_ecrit: bool


def count_by_key(df: DataFrame, column: str) -> dict[str | None, int]:
    """Nombre de lignes par valeur de ``column`` (``None`` compris)."""
    return {
        (None if row[column] is None else str(row[column])): int(row["count"])
        for row in df.groupBy(column).count().collect()
    }


def purge_staging(db: psycopg.Connection[tuple[object, ...]], batch_id: int) -> None:
    """Supprime le lot des deux tables de transit (restes d'une tentative interrompue)."""
    for table in (STAGING_TABLE, ALERTS_STAGING_TABLE):
        db.execute(f"delete from {table} where batch_id = %s", (batch_id,))


def merge_batch(
    db: psycopg.Connection[tuple[object, ...]],
    batch_id: int,
    summary: BatchSummary,
    started_at: datetime,
) -> MergeResult:
    """Fusions, purge des transits et bilan dans **une seule** transaction PostgreSQL.

    Soit tout est validé, soit rien : un bilan présent garantit que les
    transactions et les alertes du lot sont en base (I11, J7).
    """
    with db.transaction():
        nb_inseres = db.execute(MERGE_SQL, {"batch_id": batch_id}).rowcount
        nb_alertes = db.execute(MERGE_ALERTS_SQL, {"batch_id": batch_id}).rowcount
        purge_staging(db, batch_id)
        bilan = db.execute(
            INSERT_BATCH_SQL,
            {
                "batch_id": batch_id,
                "nb_lus": summary.nb_lus,
                "nb_valides": summary.nb_valides,
                "nb_inseres": max(nb_inseres, 0),
                "nb_invalides": summary.nb_invalides,
                "par_motif": json.dumps(summary.nb_invalid_by_reason),
                "par_regle": json.dumps(summary.nb_alerts_by_rule),
                "started_at": started_at,
            },
        ).rowcount
    return MergeResult(max(nb_inseres, 0), max(nb_alertes, 0), bilan == 1)


def read_profiles(batch: DataFrame, settings: PgSettings) -> DataFrame:
    """Instantané des profils, relu par JDBC à chaque lot, sans cache (J2).

    Un profil modifié en base est donc vu au lot suivant (test J12).
    """
    return (
        batch.sparkSession.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("query", PROFILES_QUERY)
        .options(**settings.jdbc_properties())
        .load()
    )


def _write_staging(df: DataFrame, table: str, settings: PgSettings) -> None:
    df.write.jdbc(settings.jdbc_url, table, mode="append", properties=settings.jdbc_properties())


def process_batch(
    batch: DataFrame, batch_id: int, settings: PgSettings, rules: RulesConfig
) -> BatchSummary:
    """Valide, applique R2 et R3, écrit et fusionne un micro-batch ; renvoie son bilan."""
    started_at = datetime.now(timezone.utc)  # début du lot côté driver (J8)
    # Lot persisté pendant toutes les actions (J9) : Kafka n'est relu qu'une fois
    validated = validate(batch).persist()
    alerts: DataFrame | None = None
    try:
        nb_lus = validated.count()  # compté, indépendant de la classification (J8)
        par_motif_brut = count_by_key(validated, "invalid_reason")
        nb_valides = par_motif_brut.pop(None, 0)
        par_motif = {
            str(k): v for k, v in sorted(par_motif_brut.items(), key=lambda kv: str(kv[0]))
        }

        # 1. Restes d'une tentative interrompue de ce même lot (deux transits)
        with settings.connect() as db:
            purge_staging(db, batch_id)

        # 2. Valides et alertes → transit, en JDBC (aucune donnée ne repasse par Python)
        valides = validated.where(F.col("invalid_reason").isNull())
        lot = F.lit(batch_id).cast("bigint").alias("batch_id")
        par_regle: dict[str, int] = {}
        if nb_valides:
            _write_staging(valides.select(lot, *STAGING_COLUMNS[1:]), STAGING_TABLE, settings)
            alerts = apply_rules(
                valides, read_profiles(batch, settings), rules.r2, rules.r3
            ).persist()
            par_regle = {str(k): v for k, v in sorted(count_by_key(alerts, "rule_id").items())}
            if par_regle:
                _write_staging(alerts.select(lot, *ALERT_COLUMNS), ALERTS_STAGING_TABLE, settings)

        summary = BatchSummary(
            batch_id=batch_id,
            nb_lus=nb_lus,
            nb_valides=nb_valides,
            nb_inseres=0,
            nb_invalid_by_reason=par_motif,
            nb_alerts_by_rule=par_regle,
        )
        # 3. Fusions, purge et bilan dans UNE SEULE transaction PostgreSQL
        with settings.connect() as db:
            resultat = merge_batch(db, batch_id, summary, started_at)
        return BatchSummary(
            batch_id=batch_id,
            nb_lus=nb_lus,
            nb_valides=nb_valides,
            nb_inseres=resultat.nb_inseres,
            nb_invalid_by_reason=par_motif,
            nb_alerts_by_rule=par_regle,
            nb_alertes_inserees=resultat.nb_alertes_inserees,
            # Bilan déjà présent : lot déjà fusionné, rejoué après un arrêt brutal
            replay=not resultat.bilan_ecrit,
        )
    finally:
        if alerts is not None:
            alerts.unpersist()
        validated.unpersist()
