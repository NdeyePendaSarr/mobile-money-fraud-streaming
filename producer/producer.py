"""Générateur SenPay : CLI, cadence, envoi Kafka et écriture PostgreSQL (phase 2, noyau).

Deux couches (H1) :

- **le « quoi »** (pur, déterministe) : ``plan_run`` construit la population, les
  épisodes et, tranche horaire par tranche horaire, les émissions triées par
  ``emit_sim_time``. Le contenu ne dépend que de la graine, de la configuration
  et de la plage simulée, jamais de la vitesse ni de la machine ;
- **le « quand » et le « où »** : ``run_live`` cadence les envois avec
  ``SimClock``, écrit la vérité terrain **avant** chaque tranche (H13), envoie
  dans Kafka (H16) et ne clôt le run (``sim_end``, ``ended_at``) qu'après un
  ``flush()`` sans erreur. ``--dry-run`` écrit les émissions dans un fichier
  JSON Lines, sans Kafka ni base : deux runs de même graine ont le même sha256.

L'heure réelle n'entre que par le paramètre ``now`` (H3) ; le seul appel à
``datetime.now`` est le point d'entrée en bas de fichier.

Usage : ``make generate ARGS="--seed 42 --start 2026-11-23T00:00:00Z --duration 1d
--speed max --init-profiles"``.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import sys
import time
from bisect import bisect_right
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
from confluent_kafka import KafkaError, Message, Producer
from psycopg.types.json import Jsonb

from producer.anomalies import CORE_TYPES, Episode, generate_episodes
from producer.clock import SimClock, check_range, parse_duration
from producer.config import DEFAULT_CONFIG_PATH, ConfigError, GeneratorConfig, load_config
from producer.events import Emission, Transaction, Truth, format_event_time, to_ms
from producer.profiles import Population, bootstrap_profiles, build_population
from producer.traffic import generate_slice, hour_slices

TOPIC = "transactions"

# Configuration du producteur Kafka (H16, F6)
KAFKA_SETTINGS: dict[str, str | int | bool] = {
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 20,
}

# Délai maximal du flush final, en secondes
FLUSH_TIMEOUT_S = 60.0


# --- Options --------------------------------------------------------------


@dataclass(frozen=True)
class RunOptions:
    seed: int
    sim_start: datetime
    duration: timedelta
    speed: float | None  # None = --speed max (H6)
    init_profiles: bool
    dry_run: bool
    out: Path | None
    config_path: Path

    @property
    def sim_end(self) -> datetime:
        return self.sim_start + self.duration


def parse_start(text: str) -> datetime:
    """``--start`` : ISO 8601 avec fuseau explicite (``Z`` ou ``+00:00``), ramené en UTC."""
    instant = datetime.fromisoformat(text)
    if instant.tzinfo is None:
        raise ValueError(f"--start sans fuseau refusé (F9) : {text!r}, ajouter « Z »")
    return instant.astimezone(UTC)


def parse_speed(text: str) -> float | None:
    """``--speed`` : ``max`` (None, sans attente) ou facteur > 0 (60 = 1 jour en 24 min)."""
    if text == "max":
        return None
    speed = float(text)
    if not speed > 0:
        raise ValueError(f"--speed doit être > 0 ou « max » : {text!r}")
    return speed


def parse_args(argv: Sequence[str]) -> RunOptions:
    parser = argparse.ArgumentParser(prog="producer", description=__doc__.split("\n\n")[0])
    parser.add_argument("--seed", type=int, required=True, help="graine du run (H4)")
    parser.add_argument("--start", required=True, help="début simulé, ex. 2026-11-23T00:00:00Z")
    parser.add_argument("--duration", required=True, help="durée simulée : 30min, 2h, 1d, 30d")
    parser.add_argument("--speed", default="60", help="facteur de vitesse ou « max » (défaut 60)")
    parser.add_argument(
        "--init-profiles", action="store_true", help="écrit les profils d'amorçage (H9)"
    )
    parser.add_argument("--dry-run", action="store_true", help="sans Kafka ni base (H1)")
    parser.add_argument("--out", type=Path, help="fichier JSON Lines du --dry-run")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    args = parser.parse_args(argv)
    if args.dry_run and args.out is None:
        parser.error("--dry-run exige --out")
    if args.out is not None and not args.dry_run:
        parser.error("--out ne sert qu'avec --dry-run")
    try:
        options = RunOptions(
            seed=args.seed,
            sim_start=parse_start(args.start),
            duration=parse_duration(args.duration),
            speed=parse_speed(args.speed),
            init_profiles=args.init_profiles,
            dry_run=args.dry_run,
            out=args.out,
            config_path=args.config,
        )
    except ValueError as erreur:
        parser.error(str(erreur))
    return options


# --- Plan du run (pur) ------------------------------------------------------


@dataclass
class Plan:
    """Tout ce que le run va envoyer, calculé sans Kafka ni base."""

    config: GeneratorConfig
    seed: int
    sim_start: datetime
    sim_end: datetime
    population: Population
    episodes: list[Episode]
    # Transactions d'épisode, rangées par indice de tranche horaire
    _episode_txs: dict[int, list[tuple[Transaction, Truth]]] = field(default_factory=dict)

    @property
    def episodes_by_type(self) -> dict[str, int]:
        """Nombre d'épisodes par type, y compris les types à 0 (``nb_episodes``)."""
        counts = Counter(e.anomaly_type for e in self.episodes)
        return {t: counts.get(t, 0) for t in CORE_TYPES}

    def slices(self) -> Iterator[list[Emission]]:
        """Émissions tranche horaire par tranche horaire, triées par ``emit_sim_time`` (H2).

        Le tri secondaire sur ``transaction_id`` rend l'ordre total, donc stable.
        """
        rng = random.Random(f"{self.seed}:trafic")
        for index, (start_ms, end_ms) in enumerate(hour_slices(self.sim_start, self.sim_end)):
            items: list[tuple[Transaction, Truth | None]] = [
                (tx, None)
                for tx in generate_slice(rng, self.config, self.population, start_ms, end_ms)
            ]
            items.extend(self._episode_txs.get(index, []))
            items.sort(key=lambda item: (item[0].event_time, str(item[0].transaction_id)))
            yield [
                Emission(
                    emit_sim_time=tx.event_time,  # noyau : pas de retard d'émission (H14)
                    key=tx.sender_id,
                    value=tx.to_json(),
                    transaction=tx,
                    truth=truth,
                )
                for tx, truth in items
            ]


def plan_run(config: GeneratorConfig, seed: int, sim_start: datetime, sim_end: datetime) -> Plan:
    """Prépare le run : population, épisodes et leur répartition par tranche horaire."""
    population = build_population(config, seed)
    episodes = generate_episodes(config, population, sim_start, sim_end, seed)
    return plan_from_episodes(config, seed, sim_start, sim_end, population, episodes)


def plan_from_episodes(
    config: GeneratorConfig,
    seed: int,
    sim_start: datetime,
    sim_end: datetime,
    population: Population,
    episodes: list[Episode],
) -> Plan:
    """Range chaque transaction d'épisode dans la tranche horaire de **son** ``event_time``.

    Un épisode n'est donc pas lié à une tranche : une rafale commencée à hh:59:30
    déborde sur la tranche suivante, et sa vérité terrain est écrite tranche par
    tranche, juste avant l'envoi des transactions concernées (H13).
    """
    plan = Plan(config, seed, sim_start, sim_end, population, episodes)
    starts = [start for start, _ in hour_slices(sim_start, sim_end)]
    for episode in episodes:
        truth = Truth(episode.anomaly_id, episode.anomaly_type, episode.expected_rule)
        for tx in episode.transactions:
            index = bisect_right(starts, to_ms(tx.event_time)) - 1
            plan._episode_txs.setdefault(index, []).append((tx, truth))
    return plan


def run_params(config: GeneratorConfig, options: RunOptions) -> dict[str, Any]:
    """``generator_runs.params`` : de quoi rejouer le run à l'identique (H17)."""
    return {
        "seed": options.seed,
        "start": format_event_time(options.sim_start),
        "duration_s": options.duration.total_seconds(),
        "speed": "max" if options.speed is None else options.speed,
        "init_profiles": options.init_profiles,
        "config_path": str(options.config_path),
        "config": config.as_dict(),
    }


# --- Bilan ------------------------------------------------------------------


@dataclass
class RunStats:
    nb_sent: int = 0
    nb_episodes: dict[str, int] = field(default_factory=dict)
    # Compléments de fin de phase (H14, H15) : toujours 0 dans le noyau
    nb_late: int = 0
    nb_very_late: int = 0
    nb_invalid: int = 0
    nb_invalid_by_reason: dict[str, int] = field(default_factory=dict)


def write_dry_run(plan: Plan, out: Path) -> tuple[RunStats, str]:
    """Écrit toutes les émissions dans ``out`` (JSON Lines) ; renvoie le bilan et le sha256."""
    stats = RunStats(nb_episodes=plan.episodes_by_type)
    out.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with out.open("w", encoding="utf-8", newline="\n") as fichier:
        for emissions in plan.slices():
            for emission in emissions:
                line = emission.to_jsonl() + "\n"
                fichier.write(line)
                digest.update(line.encode("utf-8"))
                stats.nb_sent += 1
    return stats, digest.hexdigest()


# --- Envoi réel : PostgreSQL et Kafka --------------------------------------


class RunError(RuntimeError):
    """Run en échec : il reste sans ``ended_at`` et inutilisable (H13)."""


def _env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RunError(f"variable {name} manquante (voir .env.example)")
    return value


def _connect() -> psycopg.Connection[tuple[Any, ...]]:
    return psycopg.connect(
        host=_env("SENPAY_PG_HOST"),
        port=int(_env("SENPAY_PG_PORT")),
        user=_env("SENPAY_PG_USER"),
        password=_env("SENPAY_PG_PASSWORD"),
        dbname=_env("SENPAY_PG_DB"),
        connect_timeout=10,
    )


class DeliveryTracker:
    """Compte les accusés de livraison Kafka ; toute erreur fait échouer le run (H16)."""

    def __init__(self) -> None:
        self.delivered = 0
        self.errors: list[str] = []

    def __call__(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self.errors.append(str(error))
        else:
            self.delivered += 1


def _produce(producer: Producer, emission: Emission, tracker: DeliveryTracker) -> None:
    """Envoie une émission ; si la file locale est pleine, la vide puis réessaie.

    L'horodatage du message n'est pas passé : le client le pose à l'envoi
    (CreateTime, temps réel), c'est ``ingested_at`` (F10).
    """
    while True:
        try:
            producer.produce(
                TOPIC,
                key=emission.key.encode("utf-8"),
                value=emission.value.encode("utf-8"),
                on_delivery=tracker,
            )
            return
        except BufferError:
            producer.poll(0.5)


def _wait_until(
    deadline: datetime | None,
    now: Callable[[], datetime],
    sleep: Callable[[float], None],
    producer: Producer,
) -> None:
    """Attend l'heure réelle ``deadline`` en servant les accusés de livraison."""
    if deadline is None:  # --speed max
        return
    while (remaining := (deadline - now()).total_seconds()) > 0:
        sleep(min(remaining, 0.5))
        producer.poll(0)


def run_live(
    plan: Plan,
    options: RunOptions,
    now: Callable[[], datetime],
    sleep: Callable[[float], None],
) -> tuple[int, RunStats]:
    """Exécute le run : vérité terrain, profils, envoi cadencé, clôture. Renvoie run_id et bilan."""
    stats = RunStats(nb_episodes=plan.episodes_by_type)
    with _connect() as db:
        # Un run inachevé (sans ended_at) rend la base inutilisable (H13)
        inacheves = db.execute(
            "select count(*) from generator_runs where ended_at is null"
        ).fetchone()
        if inacheves is not None and inacheves[0] > 0:
            raise RunError("un run précédent est inachevé (ended_at vide) : faire make reset")
        row = db.execute("select max(sim_end) from generator_runs").fetchone()
        last_sim_end: datetime | None = row[0] if row is not None else None
        try:
            check_range(options.sim_start, options.sim_end, now(), last_sim_end)
        except ValueError as erreur:
            # Le message de check_range dit déjà quoi faire (make reset, autre --start)
            raise RunError(str(erreur)) from erreur

        row = db.execute(
            "insert into generator_runs (seed, sim_start, speed, params) "
            "values (%s, %s, %s, %s) returning run_id",
            (
                options.seed,
                options.sim_start,
                options.speed,
                Jsonb(run_params(plan.config, options)),
            ),
        ).fetchone()
        assert row is not None
        run_id = int(row[0])
        db.commit()

        if options.init_profiles:
            profiles = bootstrap_profiles(plan.config, plan.population, options.sim_start)
            with db.cursor() as cur:
                cur.executemany(
                    "insert into user_profiles "
                    "(user_id, median_amount_xof, nb_tx_30j, home_region, first_seen, source) "
                    "values (%s, %s, %s, %s, %s, 'GENERATEUR') "
                    "on conflict (user_id) do update set "
                    "median_amount_xof = excluded.median_amount_xof, "
                    "nb_tx_30j = excluded.nb_tx_30j, home_region = excluded.home_region, "
                    "first_seen = excluded.first_seen, source = excluded.source, "
                    "computed_at = now()",
                    [
                        (p.user_id, p.median_amount_xof, p.nb_tx_30j, p.home_region, p.first_seen)
                        for p in profiles
                    ],
                )
            db.commit()

        producer = Producer(
            {"bootstrap.servers": _env("SENPAY_KAFKA_BOOTSTRAP_SERVERS"), **KAFKA_SETTINGS}
        )
        tracker = DeliveryTracker()
        clock = SimClock(sim_start=options.sim_start, speed=options.speed, real_start=now())
        for emissions in plan.slices():
            # Vérité terrain de la tranche écrite et validée AVANT l'envoi (H13)
            truths = [
                (
                    e.truth.anomaly_id,
                    e.transaction.transaction_id,
                    run_id,
                    e.truth.anomaly_type,
                    e.truth.expected_rule,
                    e.transaction.sender_id,
                    e.transaction.event_time,
                )
                for e in emissions
                if e.truth is not None
            ]
            if truths:
                with db.cursor() as cur:
                    cur.executemany(
                        "insert into injected_anomalies (anomaly_id, transaction_id, run_id, "
                        "anomaly_type, expected_rule, sender_id, event_time) "
                        "values (%s, %s, %s, %s, %s, %s, %s)",
                        truths,
                    )
                db.commit()
            for emission in emissions:
                _wait_until(clock.real_deadline(emission.emit_sim_time), now, sleep, producer)
                # Invariant F7 : jamais d'événement dans le futur réel
                if emission.transaction.event_time > now():
                    raise RunError("event_time dans le futur réel : horloge simulée en avance")
                _produce(producer, emission, tracker)
                stats.nb_sent += 1
                producer.poll(0)
            if tracker.errors:
                break

        restants = producer.flush(FLUSH_TIMEOUT_S)
        if tracker.errors or restants or tracker.delivered != stats.nb_sent:
            raise RunError(
                f"livraison Kafka incomplète : {tracker.delivered}/{stats.nb_sent} accusés, "
                f"{restants} en attente, erreurs : {tracker.errors[:3]}"
            )

        # Clôture seulement après un flush sans erreur (H13)
        db.execute(
            "update generator_runs set sim_end = %s, ended_at = now(), nb_sent = %s, "
            "nb_late = %s, nb_very_late = %s, nb_invalid = %s, nb_invalid_by_reason = %s, "
            "nb_episodes = %s where run_id = %s",
            (
                options.sim_end,
                stats.nb_sent,
                stats.nb_late,
                stats.nb_very_late,
                stats.nb_invalid,
                Jsonb(stats.nb_invalid_by_reason),
                Jsonb(stats.nb_episodes),
                run_id,
            ),
        )
        db.commit()
    return run_id, stats


# --- Point d'entrée ---------------------------------------------------------


def main(
    argv: Sequence[str],
    now: Callable[[], datetime],
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Lance un run ; renvoie 0 si tout est envoyé et clos, 1 sinon."""
    options = parse_args(argv)
    try:
        config = load_config(options.config_path)
    except ConfigError as erreur:
        print(f"ÉCHEC : configuration : {erreur}")
        return 1

    debut = time.monotonic()  # durée du run seulement, jamais une date
    plan = plan_run(config, options.seed, options.sim_start, options.sim_end)
    try:
        if options.dry_run:
            assert options.out is not None
            # Même refus qu'un vrai run pour une plage dans le futur (F7)
            check_range(options.sim_start, options.sim_end, now(), None)
            stats, sha256 = write_dry_run(plan, options.out)
            print(f"dry-run : {options.out} sha256 {sha256}")
        else:
            run_id, stats = run_live(plan, options, now, sleep)
            print(f"run {run_id} clos")
    except (ValueError, RunError, psycopg.Error) as erreur:
        print(f"ÉCHEC : {erreur}")
        return 1

    duree = time.monotonic() - debut
    debit = stats.nb_sent / max(duree, 1e-9)
    print(
        f"{stats.nb_sent} messages en {duree:.1f} s ({debit:.0f} msg/s), "
        f"épisodes {stats.nb_episodes}"
    )
    return 0


if __name__ == "__main__":
    # Seule entrée de l'heure réelle dans le générateur (H3)
    sys.exit(main(sys.argv[1:], now=lambda: datetime.now(UTC)))
