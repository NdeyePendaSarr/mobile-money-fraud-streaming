"""Épisodes injectés (§2 du cadrage de la phase 2) : R1 à R3, puis R4 et R5.

R4 (cash-out rapide) et R5 (changement de région) sont injectés dès le MVP mais
ne sont pas détectés : le rappel global reste honnêtement sous 100 %.

Un épisode = une anomalie (``anomaly_id``) et une ou plusieurs transactions. Les
épisodes sont générés à part du trafic normal (H11), puis fusionnés par temps.
Chaque type a son propre flux aléatoire (``{seed}:episodes:{TYPE}``) : changer
le nombre de rafales ne modifie ni la population, ni le trafic, ni les autres
types (H4).

Les instants suivent un processus de Poisson sur la **fenêtre horaire autorisée**
du type, avec ``per_day`` épisodes en moyenne par jour complet : un run d'une
heure a donc peu d'épisodes, et aucun de nuit s'il ne couvre pas la nuit.

Les transactions d'épisode ne sont jamais invalides ni très en retard (H12) :
``producer.delivery`` le garantit.
"""

from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime

from producer.config import GeneratorConfig
from producer.events import EXPECTED_RULE, Transaction, to_ms, uuid_from
from producer.profiles import Client, Population, expected_outgoing_30d
from producer.traffic import (
    HOUR_MS,
    draw_cash_in_amount,
    draw_outgoing_amount,
    local_windows,
    make_transaction,
    outgoing_type,
    poisson_instants,
)

DAY_MS = 24 * HOUR_MS

# Types du noyau (R1 à R3) puis compléments (R4, R5). Chaque type a son flux
# aléatoire : ajouter R4 et R5 n'a changé aucun épisode R1 à R3 d'une même graine.
CORE_TYPES: tuple[str, ...] = ("RAFALE", "MONTANT_ANORMAL", "NUIT_GROS_MONTANT")
EPISODE_TYPES: tuple[str, ...] = (*CORE_TYPES, "CASH_OUT_RAPIDE", "CHANGEMENT_REGION")

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


# --- Construction par type --------------------------------------------------
# (fenêtres horaires et processus de Poisson : producer.traffic, partagés avec
# le trafic près des seuils)


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
            rng, config, pop, client, outgoing_type(rng, config), t, amount, client.home_region
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
            rng, config, pop, client, outgoing_type(rng, config), t, amount, client.home_region
        )
        episodes.append(Episode(uuid_from(rng), "NUIT_GROS_MONTANT", (tx,)))
    return episodes


def _cash_outs_rapides(
    rng: random.Random, config: GeneratorConfig, pop: Population, start_ms: int, end_ms: int
) -> list[Episode]:
    """CASH_OUT_RAPIDE → R4 : CASH_IN de X, puis CASH_OUT d'au moins 90 % de X en 1 à 9 min.

    Les deux opérations ont lieu chez des agents de la même région (celle du
    client) ; le montant de sortie est arrondi vers le haut pour rester ≥ 90 % de X.
    """
    cfg = config.episodes.cash_out_rapide
    max_ms = cfg.delay_max_s * 1000
    windows = [(start_ms, end_ms - max_ms)] if end_ms - max_ms > start_ms else []
    episodes: list[Episode] = []
    for t in poisson_instants(rng, windows, cfg.per_day / DAY_MS):
        client = rng.choice(pop.clients)
        region = client.home_region
        x = draw_cash_in_amount(rng, config, client)
        ratio = rng.uniform(cfg.out_ratio_min, cfg.out_ratio_max)
        # ≥ 90 % de X, ≤ X, et jamais sous le minimum du schéma (X = 100 → 90 serait invalide)
        plancher = max(math.ceil(cfg.out_ratio_min * x), config.amounts.min_xof)
        sortie = min(x, max(plancher, round(ratio * x)))
        delai = rng.randint(cfg.delay_min_s * 1000, max_ms)
        cash_in = make_transaction(rng, config, pop, client, "CASH_IN", t, x, region)
        cash_out = make_transaction(rng, config, pop, client, "CASH_OUT", t + delai, sortie, region)
        episodes.append(Episode(uuid_from(rng), "CASH_OUT_RAPIDE", (cash_in, cash_out)))
    return episodes


def _changements_region(
    rng: random.Random, config: GeneratorConfig, pop: Population, start_ms: int, end_ms: int
) -> list[Episode]:
    """CHANGEMENT_REGION → R5 : 2 sorties du même client dans 2 régions éloignées, < 15 min."""
    cfg = config.episodes.changement_region
    max_ms = cfg.delay_max_s * 1000
    windows = [(start_ms, end_ms - max_ms)] if end_ms - max_ms > start_ms else []
    episodes: list[Episode] = []
    for t in poisson_instants(rng, windows, cfg.per_day / DAY_MS):
        client = rng.choice(pop.clients)
        premiere, seconde = rng.choice(cfg.far_pairs)
        if rng.random() < 0.5:  # sens du trajet tiré au hasard
            premiere, seconde = seconde, premiere
        delai = rng.randint(cfg.delay_min_s * 1000, max_ms)
        txs = tuple(
            make_transaction(
                rng,
                config,
                pop,
                client,
                outgoing_type(rng, config),
                instant,
                draw_outgoing_amount(rng, config, client),
                region,
            )
            for instant, region in ((t, premiere), (t + delai, seconde))
        )
        episodes.append(Episode(uuid_from(rng), "CHANGEMENT_REGION", txs))
    return episodes


def generate_episodes(
    config: GeneratorConfig,
    population: Population,
    sim_start: datetime,
    sim_end: datetime,
    seed: int,
) -> list[Episode]:
    """Tous les épisodes sur [sim_start ; sim_end[, triés par première transaction."""
    start_ms, end_ms = to_ms(sim_start), to_ms(sim_end)
    builders = {
        "RAFALE": _rafales,
        "MONTANT_ANORMAL": _montants_anormaux,
        "NUIT_GROS_MONTANT": _nuits_gros_montant,
        "CASH_OUT_RAPIDE": _cash_outs_rapides,
        "CHANGEMENT_REGION": _changements_region,
    }
    episodes: list[Episode] = []
    for anomaly_type in EPISODE_TYPES:
        rng = random.Random(f"{seed}:episodes:{anomaly_type}")
        episodes.extend(builders[anomaly_type](rng, config, population, start_ms, end_ms))
    episodes.sort(key=lambda e: (e.transactions[0].event_time, str(e.anomaly_id)))
    return episodes
