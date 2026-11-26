"""Écriture idempotente d'un micro-batch dans PostgreSQL (phase 3, I4 et I5).

``foreachBatch`` est *at-least-once* : après un arrêt brutal, Spark rejoue le
dernier micro-batch (mêmes offsets, même ``batch_id``). Une écriture JDBC
``append`` directe dans ``transactions`` violerait alors la clé primaire, et le
job planterait à chaque relance. D'où, pour chaque micro-batch :

1. suppression des restes éventuels du lot dans ``transactions_staging``
   (tentative précédente interrompue après l'écriture JDBC) ;
2. écriture JDBC des messages **valides** dans ``transactions_staging`` ;
3. depuis le driver, **une seule transaction** psycopg :
   ``INSERT … SELECT … ON CONFLICT (transaction_id) DO NOTHING`` dans
   ``transactions``, suppression du lot de ``transactions_staging``, bilan dans
   ``stream_batches`` (``ON CONFLICT (batch_id) DO NOTHING``).

Comme la fusion et le bilan sont validés ensemble, un bilan présent prouve que
le lot a été fusionné. Un rejeu après l'étape 3 n'insère rien et laisse le
bilan d'origine intact (``nb_inseres`` n'est jamais écrasé par le 0 du rejeu) ;
un rejeu avant l'étape 3 refait tout. Dans les deux cas : ni perte ni doublon.
Les invalides sont seulement comptés (``dead_letter`` : phase 6).
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass

import psycopg
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.validation import OUTPUT_COLUMNS, validate

STAGING_TABLE = "transactions_staging"

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

# Bilan du lot, écrit une seule fois : au rejeu d'un lot déjà fusionné, la ligne
# existe déjà (même transaction que la fusion) et garde ses comptes d'origine
INSERT_BATCH_SQL = """
insert into stream_batches (batch_id, nb_lus, nb_valides, nb_inseres, nb_invalides,
                            nb_invalid_by_reason)
values (%(batch_id)s, %(nb_lus)s, %(nb_valides)s, %(nb_inseres)s, %(nb_invalides)s,
        %(par_motif)s::jsonb)
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
    nb_lus: int
    nb_valides: int
    nb_inseres: int
    nb_invalid_by_reason: dict[str, int]
    replay: bool = False  # vrai si le lot avait déjà été fusionné (bilan d'origine conservé)

    @property
    def nb_invalides(self) -> int:
        return sum(self.nb_invalid_by_reason.values())


def count_by_reason(validated: DataFrame) -> tuple[int, dict[str, int]]:
    """Nombre de valides et nombre d'invalides par motif (motifs présents seulement)."""
    par_motif: dict[str, int] = {}
    nb_valides = 0
    for row in validated.groupBy("invalid_reason").count().collect():
        if row["invalid_reason"] is None:
            nb_valides = int(row["count"])
        else:
            par_motif[str(row["invalid_reason"])] = int(row["count"])
    return nb_valides, dict(sorted(par_motif.items()))


def process_batch(batch: DataFrame, batch_id: int, settings: PgSettings) -> BatchSummary:
    """Valide, écrit et fusionne un micro-batch ; renvoie son bilan."""
    validated = validate(batch).persist()  # plusieurs actions : ne relire Kafka qu'une fois
    try:
        nb_valides, par_motif = count_by_reason(validated)

        # 1. Restes d'une tentative interrompue de ce même lot
        with settings.connect() as db:
            db.execute(f"delete from {STAGING_TABLE} where batch_id = %s", (batch_id,))

        # 2. Valides → transit, en JDBC (aucune donnée ne repasse par Python)
        if nb_valides:
            (
                validated.where(F.col("invalid_reason").isNull())
                .select(F.lit(batch_id).cast("bigint").alias("batch_id"), *STAGING_COLUMNS[1:])
                .write.jdbc(
                    settings.jdbc_url,
                    STAGING_TABLE,
                    mode="append",
                    properties=settings.jdbc_properties(),
                )
            )

        # 3. Fusion, nettoyage et bilan dans UNE SEULE transaction PostgreSQL :
        #    soit les trois sont validés, soit aucun
        with settings.connect() as db, db.transaction():
            nb_inseres = db.execute(MERGE_SQL, {"batch_id": batch_id}).rowcount
            db.execute(f"delete from {STAGING_TABLE} where batch_id = %s", (batch_id,))
            nb_lus = nb_valides + sum(par_motif.values())
            bilan_ecrit = db.execute(
                INSERT_BATCH_SQL,
                {
                    "batch_id": batch_id,
                    "nb_lus": nb_lus,
                    "nb_valides": nb_valides,
                    "nb_inseres": max(nb_inseres, 0),
                    "nb_invalides": sum(par_motif.values()),
                    "par_motif": json.dumps(par_motif),
                },
            ).rowcount
        return BatchSummary(
            batch_id=batch_id,
            nb_lus=nb_lus,
            nb_valides=nb_valides,
            nb_inseres=max(nb_inseres, 0),
            nb_invalid_by_reason=par_motif,
            # Bilan déjà présent : lot déjà fusionné, rejoué après un arrêt brutal
            replay=bilan_ecrit == 0,
        )
    finally:
        validated.unpersist()
