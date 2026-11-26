"""Chiffres du critère de fin de la phase 3 (``make stream-report``).

Compare ce que le générateur a envoyé (``generator_runs``) à ce que le
consommateur Spark a lu, inséré et écarté (``transactions``, ``stream_batches``) :

- Σ ``nb_sent`` = ``COUNT(transactions)`` + Σ ``stream_batches.nb_invalides`` ;
- Σ ``nb_lus`` = Σ ``nb_sent`` (chaque message lu une fois, rejeux compris) ;
- Σ ``nb_inseres`` = ``COUNT(transactions)`` (un rejeu n'écrase jamais un bilan) ;
- invalides par motif : générateur = consommateur ;
- aucun doublon, ni par ``transaction_id`` ni par (partition, offset) ;
- ``transactions_staging`` vide (aucun lot à moitié fusionné).

Code de sortie : 0 si tout concorde, 1 sinon, 2 si la base est injoignable.
Le job doit avoir rattrapé tout le topic avant la lecture (voir ``make stream-logs``).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import psycopg

QUERIES: dict[str, str] = {
    "runs": "select count(*), count(*) filter (where ended_at is null), "
    "coalesce(sum(nb_sent), 0) from generator_runs",
    "transactions": "select count(*), count(distinct transaction_id), "
    "count(distinct (kafka_partition, kafka_offset)) from transactions",
    "batches": "select count(*), coalesce(sum(nb_lus), 0), coalesce(sum(nb_valides), 0), "
    "coalesce(sum(nb_inseres), 0), coalesce(sum(nb_invalides), 0) from stream_batches",
    "staging": "select count(*) from transactions_staging",
    "motifs_generateur": "select key, sum(value::bigint) from generator_runs, "
    "jsonb_each_text(nb_invalid_by_reason) group by key",
    "motifs_consommateur": "select key, sum(value::bigint) from stream_batches, "
    "jsonb_each_text(nb_invalid_by_reason) group by key",
}


@dataclass(frozen=True)
class Report:
    """Chiffres lus en base, et contrôles qui en découlent."""

    nb_runs: int
    nb_runs_inacheves: int
    nb_sent: int
    nb_transactions: int
    nb_transaction_ids: int
    nb_positions: int
    nb_lots: int
    nb_lus: int
    nb_valides: int
    nb_inseres: int
    nb_invalides: int
    nb_staging: int
    motifs_generateur: dict[str, int]
    motifs_consommateur: dict[str, int]

    def ecarts(self) -> list[str]:
        """Écarts au critère (vide = critère rempli)."""
        ecarts: list[str] = []
        if self.nb_runs == 0:
            ecarts.append("aucun run du générateur en base")
        if self.nb_runs_inacheves:
            ecarts.append(f"{self.nb_runs_inacheves} run(s) inachevé(s) (ended_at vide)")
        if self.nb_sent != self.nb_transactions + self.nb_invalides:
            ecarts.append(
                f"envoyés {self.nb_sent} ≠ transactions {self.nb_transactions} "
                f"+ invalides {self.nb_invalides}"
            )
        if self.nb_lus != self.nb_sent:
            ecarts.append(f"lus {self.nb_lus} ≠ envoyés {self.nb_sent} (rattrapage en cours ?)")
        if not self.nb_transactions == self.nb_transaction_ids == self.nb_positions:
            ecarts.append("doublons dans transactions")
        if self.nb_inseres != self.nb_transactions:
            ecarts.append(f"Σ nb_inseres {self.nb_inseres} ≠ transactions {self.nb_transactions}")
        if self.nb_staging:
            ecarts.append(f"{self.nb_staging} ligne(s) restée(s) dans transactions_staging")
        gen = {k: v for k, v in self.motifs_generateur.items() if v}
        if gen != self.motifs_consommateur:
            ecarts.append(f"invalides par motif : générateur {gen} ≠ consommateur")
        return ecarts


def read_report(db: psycopg.Connection[tuple[Any, ...]]) -> Report:
    """Lit tous les chiffres dans la même transaction (lecture cohérente)."""

    def one(name: str) -> tuple[Any, ...]:
        row = db.execute(QUERIES[name]).fetchone()
        assert row is not None
        return row

    def by_key(name: str) -> dict[str, int]:
        return {str(k): int(v) for k, v in db.execute(QUERIES[name]).fetchall()}

    runs, txs, lots = one("runs"), one("transactions"), one("batches")
    return Report(
        nb_runs=int(runs[0]),
        nb_runs_inacheves=int(runs[1]),
        nb_sent=int(runs[2]),
        nb_transactions=int(txs[0]),
        nb_transaction_ids=int(txs[1]),
        nb_positions=int(txs[2]),
        nb_lots=int(lots[0]),
        nb_lus=int(lots[1]),
        nb_valides=int(lots[2]),
        nb_inseres=int(lots[3]),
        nb_invalides=int(lots[4]),
        nb_staging=int(one("staging")[0]),
        motifs_generateur=dict(sorted(by_key("motifs_generateur").items())),
        motifs_consommateur=dict(sorted(by_key("motifs_consommateur").items())),
    )


def print_report(report: Report) -> None:
    print(f"runs du générateur      : {report.nb_runs} (inachevés : {report.nb_runs_inacheves})")
    print(f"Σ nb_sent               : {report.nb_sent}")
    print(f"micro-batchs            : {report.nb_lots}")
    print(f"Σ nb_lus                : {report.nb_lus}")
    print(f"COUNT(transactions)     : {report.nb_transactions}")
    print(f"Σ nb_invalides          : {report.nb_invalides}")
    total = report.nb_transactions + report.nb_invalides
    print(f"transactions + invalides: {total}")
    egal = "=" if report.nb_inseres == report.nb_transactions else "≠"
    print(f"Σ nb_inseres            : {report.nb_inseres} ({egal} COUNT(transactions))")
    print(f"doublons                : {report.nb_transactions - report.nb_transaction_ids}")
    print(f"restes de transit       : {report.nb_staging}")
    print("invalides par motif     : générateur | consommateur")
    for motif in sorted(set(report.motifs_generateur) | set(report.motifs_consommateur)):
        gen = report.motifs_generateur.get(motif, 0)
        cons = report.motifs_consommateur.get(motif, 0)
        print(f"  {motif:<24} {gen:>6} | {cons:<6} {'' if gen == cons else '← écart'}")


def main(env: Mapping[str, str] = os.environ) -> int:
    try:
        with psycopg.connect(
            host=env["SENPAY_PG_HOST"],
            port=int(env["SENPAY_PG_PORT"]),
            user=env["SENPAY_PG_USER"],
            password=env["SENPAY_PG_PASSWORD"],
            dbname=env["SENPAY_PG_DB"],
            connect_timeout=10,
        ) as db:
            report = read_report(db)
    except (KeyError, psycopg.OperationalError) as erreur:
        print(f"ÉCHEC : base injoignable ou configuration manquante : {erreur}")
        return 2
    print_report(report)
    ecarts = report.ecarts()
    for ecart in ecarts:
        print(f"ÉCHEC : {ecart}")
    if not ecarts:
        print("OK : critère de la phase 3 rempli")
    return 1 if ecarts else 0


if __name__ == "__main__":
    sys.exit(main())
