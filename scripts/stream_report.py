"""Chiffres du critère des phases 3 et 4 (``make stream-report [ARGS="--run N"]``).

**Invariants du consommateur** (phase 3, toute la base ; ils fixent le code de sortie) :

- Σ ``nb_sent`` = ``COUNT(transactions)`` + Σ ``stream_batches.nb_invalides`` ;
- Σ ``nb_lus`` (compté sur chaque lot, J8) = Σ ``nb_sent`` ;
- Σ ``nb_inseres`` = ``COUNT(transactions)`` (un rejeu n'écrase jamais un bilan) ;
- invalides par motif : générateur = consommateur ;
- aucun doublon ; ``transactions_staging`` et ``alerts_staging`` vides.

**Détection** (phase 4, J10 et J11 ; informatif) sur une fenêtre de temps réel :
``--run N`` (de ``started_at`` à ``ended_at`` du run N), ou ``--since`` / ``--until``
(ISO 8601), sinon toute la base :

- alertes par règle ; **vrai positif** = ``transaction_id`` de l'alerte présent dans
  ``injected_anomalies``, quel que soit le type (phase 0 §4) ; faux positifs ;
- **rappel par type attendu** : ``MONTANT_ANORMAL`` → R2, ``NUIT_GROS_MONTANT`` → R3
  (épisodes du run N, sinon de tous les runs), avec les épisodes ratés ;
- **latence** = ``detected_at − ingested_at`` par règle : n, médiane, p95, max ;
  durée des lots (``processed_at − started_at``) : n, médiane, max. La latence n'a de
  sens que sur un run en direct (pas sur un arriéré, J10).

Code de sortie : 0 si les invariants concordent, 1 sinon, 2 si la base est injoignable.
Le job doit avoir rattrapé tout le topic avant la lecture (voir ``make stream-logs``).
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg

# Types d'épisodes que les règles de la phase 4 doivent attraper
RAPPEL_ATTENDU: tuple[tuple[str, str], ...] = (
    ("MONTANT_ANORMAL", "R2"),
    ("NUIT_GROS_MONTANT", "R3"),
)

# Fenêtre de temps réel : bornes nulles = pas de borne
FENETRE = (
    "ingested_at >= coalesce(%(debut)s::timestamptz, '-infinity') "
    "and ingested_at <= coalesce(%(fin)s::timestamptz, 'infinity')"
)

DETECTION_QUERIES: dict[str, str] = {
    "alertes": "select a.rule_id, count(*), count(*) filter (where exists "
    "(select 1 from injected_anomalies i where i.transaction_id = a.transaction_id)) "
    f"from alerts a where {FENETRE} group by a.rule_id order by a.rule_id",
    "rappel": "select count(distinct i.anomaly_id), count(distinct i.anomaly_id) filter "
    "(where exists (select 1 from alerts a where a.transaction_id = i.transaction_id "
    "and a.rule_id = %(regle)s)) from injected_anomalies i "
    "where i.anomaly_type = %(type)s and (%(run)s::bigint is null or i.run_id = %(run)s)",
    "rates": "select i.anomaly_id, i.transaction_id, i.event_time from injected_anomalies i "
    "where i.anomaly_type = %(type)s and (%(run)s::bigint is null or i.run_id = %(run)s) "
    "and not exists (select 1 from alerts a where a.transaction_id = i.transaction_id "
    "and a.rule_id = %(regle)s) order by i.event_time limit 5",
    "latence": "select rule_id, count(*), "
    "percentile_cont(0.5) within group (order by extract(epoch from detected_at - ingested_at)), "
    "percentile_cont(0.95) within group (order by extract(epoch from detected_at - ingested_at)), "
    "max(extract(epoch from detected_at - ingested_at)) "
    f"from alerts where {FENETRE} group by rule_id order by rule_id",
    # Lots démarrés dans la fenêtre (jusqu'à 1 min après la fin : derniers messages)
    # started_at (horloge du driver) et processed_at (horloge de PostgreSQL) viennent de
    # deux horloges : une durée négative est signalée, jamais bloquante (pas de CHECK)
    "lots": "select count(*), "
    "percentile_cont(0.5) within group (order by extract(epoch from processed_at - started_at)), "
    "max(extract(epoch from processed_at - started_at)), "
    "count(*) filter (where processed_at < started_at), "
    "min(extract(epoch from processed_at - started_at)) from stream_batches "
    "where started_at >= coalesce(%(debut)s::timestamptz, '-infinity') "
    "and started_at <= coalesce(%(fin)s::timestamptz + interval '1 minute', 'infinity')",
}

QUERIES: dict[str, str] = {
    "runs": "select count(*), count(*) filter (where ended_at is null), "
    "coalesce(sum(nb_sent), 0) from generator_runs",
    "transactions": "select count(*), count(distinct transaction_id), "
    "count(distinct (kafka_partition, kafka_offset)) from transactions",
    "batches": "select count(*), coalesce(sum(nb_lus), 0), coalesce(sum(nb_valides), 0), "
    "coalesce(sum(nb_inseres), 0), coalesce(sum(nb_invalides), 0) from stream_batches",
    "staging": "select (select count(*) from transactions_staging) "
    "+ (select count(*) from alerts_staging)",
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
            ecarts.append(f"{self.nb_staging} ligne(s) restée(s) dans les tables de transit")
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


# --- Détection (phase 4) --------------------------------------------------------


@dataclass(frozen=True)
class Fenetre:
    """Fenêtre de temps réel des chiffres de détection ; bornes ``None`` = ouvertes."""

    debut: datetime | None
    fin: datetime | None
    run_id: int | None
    libelle: str


def window_for(
    db: psycopg.Connection[tuple[Any, ...]],
    run_id: int | None,
    since: datetime | None,
    until: datetime | None,
) -> Fenetre:
    """Fenêtre du run ``run_id`` (started_at → ended_at), ou bornes données."""
    if run_id is None:
        libelle = "toute la base" if since is None and until is None else "fenêtre donnée"
        return Fenetre(since, until, None, libelle)
    row = db.execute(
        "select started_at, ended_at from generator_runs where run_id = %s", (run_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"run {run_id} absent de generator_runs")
    return Fenetre(row[0], row[1], run_id, f"run {run_id}")


def _secondes(value: Any) -> str:
    return "—" if value is None else f"{float(value):.2f} s"


def print_detection(db: psycopg.Connection[tuple[Any, ...]], fenetre: Fenetre) -> None:
    """Alertes, vrais et faux positifs, rappel par type attendu, latence, durée des lots."""
    params: dict[str, Any] = {"debut": fenetre.debut, "fin": fenetre.fin, "run": fenetre.run_id}
    bornes = f"{fenetre.debut} → {fenetre.fin}" if fenetre.debut or fenetre.fin else "sans borne"
    print(f"\n--- détection : {fenetre.libelle} ({bornes})")

    print("alertes par règle       : total | vrais positifs | faux positifs | précision")
    for regle, total, vrais in db.execute(DETECTION_QUERIES["alertes"], params).fetchall():
        precision = f"{100 * vrais / total:.1f} %" if total else "—"
        print(f"  {regle:<4} {total:>18} | {vrais:>14} | {total - vrais:>13} | {precision}")

    print("rappel par type attendu :")
    for anomaly_type, regle in RAPPEL_ATTENDU:
        p = {**params, "type": anomaly_type, "regle": regle}
        row = db.execute(DETECTION_QUERIES["rappel"], p).fetchone()
        assert row is not None
        episodes, detectes = int(row[0]), int(row[1])
        rappel = f"{100 * detectes / episodes:.1f} %" if episodes else "—"
        print(f"  {anomaly_type:<18} → {regle} : {detectes}/{episodes} épisodes ({rappel})")
        for anomaly_id, tx_id, event_time in db.execute(DETECTION_QUERIES["rates"], p).fetchall():
            print(f"    raté : épisode {anomaly_id}, transaction {tx_id}, event_time {event_time}")

    print("latence detected_at − ingested_at : n | médiane | p95 | max")
    for regle, n, mediane, p95, maximum in db.execute(
        DETECTION_QUERIES["latence"], params
    ).fetchall():
        print(
            f"  {regle:<4} {n:>6} | {_secondes(mediane)} | {_secondes(p95)} | {_secondes(maximum)}"
        )
    lots = db.execute(DETECTION_QUERIES["lots"], params).fetchone()
    assert lots is not None
    mediane, maximum = _secondes(lots[1]), _secondes(lots[2])
    print(f"durée des lots          : n {lots[0]} | médiane {mediane} | max {maximum}")
    if lots[3]:
        print(
            f"ATTENTION : {lots[3]} lot(s) de durée négative (min {_secondes(lots[4])}) : "
            "écart entre l'horloge du driver et celle de PostgreSQL"
        )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="stream_report", description=__doc__.split("\n\n")[0])
    parser.add_argument("--run", type=int, help="fenêtre du run N (temps réel d'envoi)")
    parser.add_argument("--since", type=datetime.fromisoformat, help="début, ISO 8601 avec fuseau")
    parser.add_argument("--until", type=datetime.fromisoformat, help="fin, ISO 8601 avec fuseau")
    args = parser.parse_args(argv)
    if args.run is not None and (args.since or args.until):
        parser.error("--run ou --since/--until, pas les deux")
    return args


def main(argv: Sequence[str] = (), env: Mapping[str, str] = os.environ) -> int:
    args = parse_args(argv)
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
            print_report(report)
            print_detection(db, window_for(db, args.run, args.since, args.until))
    except (KeyError, psycopg.OperationalError) as erreur:
        print(f"ÉCHEC : base injoignable ou configuration manquante : {erreur}")
        return 2
    except ValueError as erreur:
        print(f"ÉCHEC : {erreur}")
        return 2
    ecarts = report.ecarts()
    for ecart in ecarts:
        print(f"ÉCHEC : {ecart}")
    if not ecarts:
        print("OK : invariants du consommateur vérifiés")
    return 1 if ecarts else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
