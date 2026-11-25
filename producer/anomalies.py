"""Épisodes injectés du noyau (§2 du cadrage de la phase 2) : R1, R2, R3.

Un épisode = une anomalie (``anomaly_id``) et une ou plusieurs transactions. Les
épisodes sont générés à part du trafic normal (H11), puis fusionnés par temps.
Chaque type a son propre flux aléatoire (``{seed}:episodes:{TYPE}``) : changer
le nombre de rafales ne modifie ni la population, ni le trafic, ni les autres
types (H4).

Les instants suivent un processus de Poisson sur la **fenêtre horaire autorisée**
du type, avec ``per_day`` épisodes en moyenne par jour complet : un run d'une
heure a donc peu d'épisodes, et aucun de nuit s'il ne couvre pas la nuit.

Les transactions d'épisode ne sont jamais invalides ni très en retard (H12) :
ces compléments ne s'appliqueront qu'au trafic normal.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from producer.config import GeneratorConfig
from producer.events import (
    EXPECTED_RULE,
    OUTGOING_TYPES,
    Transaction,
    from_ms,
    to_ms,
    uuid_from,
)
from producer.profiles import Client, Population, expected_outgoing_30d
from producer.traffic import HOUR_MS, draw_outgoing_amount, make_transaction

DAY_MS = 24 * HOUR_MS

# Types du noyau, dans un ordre fixe (R4 et R5 : compléments de fin de phase)
CORE_TYPES: tuple[str, ...] = ("RAFALE", "MONTANT_ANORMAL", "NUIT_GROS_MONTANT")

# Nombre maximal de clients essayés pour un MONTANT_ANORMAL sous le plafond
_MAX_ESSAIS = 1_000


@dataclass(frozen=True)
class Episode:
    anomaly_id: uuid.UUID
    anomaly_type: str
    transactions: tuple[Transaction, ...]

    @property
    def expected_rule(self) -> str:
        return EXPECTED_RULE[self.anomaly_type]


# --- Fenêtres horaires et processus de Poisson ------------------------------


def local_windows(
    start_ms: int, end_ms: int, tz: ZoneInfo, hour_start: int, hour_end: int
) -> list[tuple[int, int]]:
    """Plages [hour_start ; hour_end[ en heure locale, intersectées avec [start ; end[ (ms)."""
    first_day = from_ms(start_ms).astimezone(tz).date() - timedelta(days=1)
    last_day = from_ms(end_ms).astimezone(tz).date()
    windows: list[tuple[int, int]] = []
    day = first_day
    while day <= last_day:
        midnight = datetime(day.year, day.month, day.day, tzinfo=tz)
        lo = to_ms(midnight + timedelta(hours=hour_start))
        hi = to_ms(midnight + timedelta(hours=hour_end))
        lo, hi = max(lo, start_ms), min(hi, end_ms)
        if lo < hi:
            windows.append((lo, hi))
        day += timedelta(days=1)
    return windows


def poisson_instants(rng: random.Random, windows: list[tuple[int, int]], rate: float) -> list[int]:
    """Instants (ms) d'un processus de Poisson d'intensité ``rate`` (par ms) sur les fenêtres."""
    instants: list[int] = []
    if rate <= 0:
        return instants
    for lo, hi in windows:
        t = float(lo)
        while True:
            t += rng.expovariate(rate)
            if t >= hi:
                break
            instants.append(int(t))
    return instants


def _outgoing_type(rng: random.Random, config: GeneratorConfig) -> str:
    """Type sortant tiré selon le mix du trafic normal."""
    weights = [config.traffic.type_weights.get(t, 0.0) for t in OUTGOING_TYPES]
    return rng.choices(OUTGOING_TYPES, weights)[0]


# --- Construction par type --------------------------------------------------


def _rafales(
    rng: random.Random, config: GeneratorConfig, pop: Population, start_ms: int, end_ms: int
) -> list[Episode]:
    """RAFALE → R1 : 6 à 10 TRANSFER du même client en au plus 90 s (F17)."""
    cfg = config.episodes.rafale
    max_ms = cfg.max_duration_s * 1000
    # Le début laisse la place à toute la rafale avant la fin du run
    windows = [(start_ms, end_ms - max_ms)] if end_ms - max_ms > start_ms else []
    episodes: list[Episode] = []
    for t0 in poisson_instants(rng, windows, cfg.per_day / DAY_MS):
        client = rng.choice(pop.clients)
        n = rng.randint(cfg.min_transfers, cfg.max_transfers)
        duration = rng.randint(min(n * 1000, max_ms), max_ms)
        offsets = sorted([0, duration, *(rng.randint(0, duration) for _ in range(n - 2))])
        # Destinataires variés : n clients distincts, jamais l'émetteur
        receivers = [c.user_id for c in rng.sample(pop.clients, n + 1) if c is not client][:n]
        channel = rng.choice(("APP", "USSD"))
        txs = tuple(
            make_transaction(
                rng,
                config,
                pop,
                client,
                "TRANSFER",
                t0 + offset,
                draw_outgoing_amount(rng, config, client),
                client.home_region,
                channel=channel,
                receiver_id=receiver,
            )
            for offset, receiver in zip(offsets, receivers, strict=True)
        )
        episodes.append(Episode(uuid_from(rng), "RAFALE", txs))
    return episodes


def _montants_anormaux(
    rng: random.Random, config: GeneratorConfig, pop: Population, start_ms: int, end_ms: int
) -> list[Episode]:
    """MONTANT_ANORMAL → R2 : une sortie à 15–50 × la médiane, de jour (§2)."""
    cfg = config.episodes.montant_anormal
    windows = local_windows(start_ms, end_ms, config.tz, cfg.hour_start, cfg.hour_end)
    rate = cfg.per_day / ((cfg.hour_end - cfg.hour_start) * HOUR_MS)
    # Clients avec assez d'historique pour que R2 s'applique (F14)
    eligibles: list[Client] = [
        c for c in pop.clients if expected_outgoing_30d(config, c) >= cfg.min_history
    ]
    episodes: list[Episode] = []
    for t in poisson_instants(rng, windows, rate):
        for _ in range(_MAX_ESSAIS):
            client = rng.choice(eligibles)
            factor = rng.uniform(cfg.factor_min, cfg.factor_max)
            amount = max(cfg.floor_xof, round(client.median_amount_xof * factor))
            if amount <= config.amounts.max_xof:
                break
        else:
            raise RuntimeError("MONTANT_ANORMAL : aucun client sous le plafond de montant")
        tx = make_transaction(
            rng, config, pop, client, _outgoing_type(rng, config), t, amount, client.home_region
        )
        episodes.append(Episode(uuid_from(rng), "MONTANT_ANORMAL", (tx,)))
    return episodes


def _nuits_gros_montant(
    rng: random.Random, config: GeneratorConfig, pop: Population, start_ms: int, end_ms: int
) -> list[Episode]:
    """NUIT_GROS_MONTANT → R3 : une sortie de 500 001 à 2 000 000 XOF entre 1 h et 5 h."""
    cfg = config.episodes.nuit_gros_montant
    windows = local_windows(start_ms, end_ms, config.tz, cfg.hour_start, cfg.hour_end)
    rate = cfg.per_day / ((cfg.hour_end - cfg.hour_start) * HOUR_MS)
    episodes: list[Episode] = []
    for t in poisson_instants(rng, windows, rate):
        client = rng.choice(pop.clients)
        amount = rng.randint(cfg.amount_min_xof, cfg.amount_max_xof)
        tx = make_transaction(
            rng, config, pop, client, _outgoing_type(rng, config), t, amount, client.home_region
        )
        episodes.append(Episode(uuid_from(rng), "NUIT_GROS_MONTANT", (tx,)))
    return episodes


def generate_episodes(
    config: GeneratorConfig,
    population: Population,
    sim_start: datetime,
    sim_end: datetime,
    seed: int,
) -> list[Episode]:
    """Tous les épisodes du noyau sur [sim_start ; sim_end[, triés par première transaction."""
    start_ms, end_ms = to_ms(sim_start), to_ms(sim_end)
    builders = {
        "RAFALE": _rafales,
        "MONTANT_ANORMAL": _montants_anormaux,
        "NUIT_GROS_MONTANT": _nuits_gros_montant,
    }
    episodes: list[Episode] = []
    for anomaly_type in CORE_TYPES:
        rng = random.Random(f"{seed}:episodes:{anomaly_type}")
        episodes.extend(builders[anomaly_type](rng, config, population, start_ms, end_ms))
    episodes.sort(key=lambda e: (e.transactions[0].event_time, str(e.anomaly_id)))
    return episodes
