"""Trafic normal (H8) : fonctions pures, déterministes pour une graine donnée.

Le trafic est produit **par tranche d'une heure simulée** (H2) : la mémoire reste
bornée même pour 30 jours. Dans une tranche, les arrivées suivent un processus
de Poisson d'intensité constante (``tx_per_day`` × poids de l'heure locale) :
les écarts entre deux transactions sont exponentiels. Le processus est sans
mémoire, donc repartir de zéro à chaque tranche ne biaise rien.

Le trafic près des seuils (H10) est un complément de fin de phase : absent ici.
"""

from __future__ import annotations

import random
from datetime import datetime
from zoneinfo import ZoneInfo

from producer.config import GeneratorConfig
from producer.events import REGIONS, Transaction, from_ms, to_ms, uuid_from
from producer.profiles import Client, Population

HOUR_MS = 3_600_000


def local_hour(epoch_ms: int, tz: ZoneInfo) -> int:
    """Heure locale (0–23) d'un instant, dans le fuseau métier (F9)."""
    return from_ms(epoch_ms).astimezone(tz).hour


def hour_slices(sim_start: datetime, sim_end: datetime) -> list[tuple[int, int]]:
    """Découpe [sim_start ; sim_end[ en tranches alignées sur les heures pleines (ms)."""
    start, end = to_ms(sim_start), to_ms(sim_end)
    slices: list[tuple[int, int]] = []
    cursor = start
    while cursor < end:
        next_hour = (cursor // HOUR_MS + 1) * HOUR_MS
        slices.append((cursor, min(next_hour, end)))
        cursor = next_hour
    return slices


def clamp_amount(config: GeneratorConfig, value: float) -> int:
    """Arrondit et borne un montant à [min_xof ; max_xof]."""
    return min(max(round(value), config.amounts.min_xof), config.amounts.max_xof)


def draw_outgoing_amount(rng: random.Random, config: GeneratorConfig, client: Client) -> int:
    """Montant sortant : même loi log-normale pour TRANSFER, PAYMENT et CASH_OUT (H8).

    Borner après tirage ne déplace pas la médiane, puisque exp(mu) est déjà dans
    les bornes (``build_population``).
    """
    return clamp_amount(config, rng.lognormvariate(client.mu, config.population.amount_sigma))


def draw_cash_in_amount(rng: random.Random, config: GeneratorConfig, client: Client) -> int:
    """Montant de CASH_IN : sa propre loi (mu décalé), sans effet sur les profils."""
    pop = config.population
    return clamp_amount(
        config, rng.lognormvariate(client.mu + pop.cash_in_mu_shift, pop.amount_sigma)
    )


def pick_region(rng: random.Random, config: GeneratorConfig, client: Client) -> str:
    """Région habituelle du client dans ``home_region_share`` des cas, sinon une autre."""
    if rng.random() < config.traffic.home_region_share:
        return client.home_region
    return rng.choice([r for r in REGIONS if r != client.home_region])


def make_transaction(
    rng: random.Random,
    config: GeneratorConfig,
    population: Population,
    client: Client,
    tx_type: str,
    epoch_ms: int,
    amount_xof: int,
    region: str,
    channel: str | None = None,
    receiver_id: str | None = None,
) -> Transaction:
    """Construit une transaction conforme à la matrice type × canal (§2.2).

    ``channel`` et ``receiver_id`` ne servent qu'aux TRANSFER et PAYMENT (tirés
    s'ils sont absents) ; CASH_IN et CASH_OUT passent toujours par un agent de
    ``region``, sans destinataire.
    """
    receiver: str | None = None
    agent: str | None = None
    if tx_type in ("TRANSFER", "PAYMENT"):
        if channel is None:
            weights = config.traffic.channel_weights
            channel = rng.choices(list(weights), list(weights.values()))[0]
        if receiver_id is not None:
            receiver = receiver_id
        elif tx_type == "TRANSFER":
            # Destinataire : un autre client (jamais l'émetteur lui-même)
            while True:
                other = rng.choice(population.clients)
                if other.user_id != client.user_id:
                    receiver = other.user_id
                    break
        else:
            receiver = rng.choice(population.merchants)
    else:
        channel = "AGENT"
        agent = rng.choice(population.agents_by_region[region])
    return Transaction(
        transaction_id=uuid_from(rng),
        event_time=from_ms(epoch_ms),
        sender_id=client.user_id,
        receiver_id=receiver,
        type=tx_type,
        amount_xof=amount_xof,
        region=region,
        channel=channel,
        agent_id=agent,
    )


def generate_slice(
    rng: random.Random,
    config: GeneratorConfig,
    population: Population,
    start_ms: int,
    end_ms: int,
) -> list[Transaction]:
    """Trafic normal de la tranche [start_ms ; end_ms[, trié par ``event_time``."""
    traffic = config.traffic
    weight = traffic.hourly_weights[local_hour(start_ms, config.tz)]
    # Intensité en transactions par milliseconde pour cette heure locale
    rate = traffic.tx_per_day * weight / sum(traffic.hourly_weights) / HOUR_MS
    if rate <= 0:
        return []
    types = list(traffic.type_weights)
    type_weights = list(traffic.type_weights.values())

    transactions: list[Transaction] = []
    t = float(start_ms)
    while True:
        t += rng.expovariate(rate)
        if t >= end_ms:
            break
        client = population.pick_client(rng)
        tx_type = rng.choices(types, type_weights)[0]
        if tx_type == "CASH_IN":
            amount = draw_cash_in_amount(rng, config, client)
        else:
            amount = draw_outgoing_amount(rng, config, client)
        region = pick_region(rng, config, client)
        transactions.append(
            make_transaction(rng, config, population, client, tx_type, int(t), amount, region)
        )
    return transactions
