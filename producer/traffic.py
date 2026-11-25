"""Trafic normal (H8) : fonctions pures, déterministes pour une graine donnée.

Le trafic est produit **par tranche d'une heure simulée** (H2) : la mémoire reste
bornée même pour 30 jours. Dans une tranche, les arrivées suivent un processus
de Poisson d'intensité constante (``tx_per_day`` × poids de l'heure locale) :
les écarts entre deux transactions sont exponentiels. Le processus est sans
mémoire, donc repartir de zéro à chaque tranche ne biaise rien.

Le **trafic près des seuils** (H10, ``generate_near_threshold``) est du trafic
normal, absent de la vérité terrain : il rend les faux positifs possibles. Chaque
sorte a son flux aléatoire (``{seed}:near_threshold:{SORTE}``), indépendant du
trafic ordinaire et des épisodes.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from producer.config import GeneratorConfig
from producer.events import OUTGOING_TYPES, REGIONS, Transaction, from_ms, to_ms, uuid_from
from producer.profiles import Client, Population, expected_outgoing_30d

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS


def local_hour(epoch_ms: int, tz: ZoneInfo) -> int:
    """Heure locale (0–23) d'un instant, dans le fuseau métier (F9)."""
    return from_ms(epoch_ms).astimezone(tz).hour


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


def outgoing_type(rng: random.Random, config: GeneratorConfig) -> str:
    """Type sortant tiré selon le mix du trafic normal."""
    weights = [config.traffic.type_weights.get(t, 0.0) for t in OUTGOING_TYPES]
    return rng.choices(OUTGOING_TYPES, weights)[0]


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


# --- Trafic près des seuils (H10) -------------------------------------------

NEAR_THRESHOLD_KINDS: tuple[str, ...] = (
    "PRESQUE_RAFALE",
    "GROS_MONTANT_JOUR",
    "NUIT_SOUS_SEUIL",
    "MULTIPLE_MEDIANE",
)


def _presque_rafales(
    rng: random.Random, config: GeneratorConfig, pop: Population, start_ms: int, end_ms: int
) -> list[Transaction]:
    """5 TRANSFER du même client en moins de 2 min : un transfert de plus suffit à R1."""
    cfg = config.near_threshold
    max_ms = cfg.presque_rafale_max_duration_s * 1000
    n = cfg.presque_rafale_transfers
    windows = [(start_ms, end_ms - max_ms)] if end_ms - max_ms > start_ms else []
    txs: list[Transaction] = []
    for t0 in poisson_instants(rng, windows, cfg.presque_rafale_per_day / DAY_MS):
        client = rng.choice(pop.clients)
        duration = rng.randint(min(n * 1000, max_ms), max_ms)
        offsets = sorted([0, duration, *(rng.randint(0, duration) for _ in range(n - 2))])
        receivers = [c.user_id for c in rng.sample(pop.clients, n + 1) if c is not client][:n]
        channel = rng.choice(("APP", "USSD"))
        for offset, receiver in zip(offsets, receivers, strict=True):
            amount = draw_outgoing_amount(rng, config, client)
            txs.append(
                make_transaction(
                    rng,
                    config,
                    pop,
                    client,
                    "TRANSFER",
                    t0 + offset,
                    amount,
                    client.home_region,
                    channel=channel,
                    receiver_id=receiver,
                )
            )
    return txs


def _montants_fixes(
    rng: random.Random,
    config: GeneratorConfig,
    pop: Population,
    windows: list[tuple[int, int]],
    rate: float,
    amount_min: int,
    amount_max: int,
) -> list[Transaction]:
    """Sorties à montant uniforme dans [amount_min ; amount_max], aux instants d'un Poisson."""
    txs: list[Transaction] = []
    for t in poisson_instants(rng, windows, rate):
        client = rng.choice(pop.clients)
        amount = rng.randint(amount_min, amount_max)
        tx_type = outgoing_type(rng, config)
        txs.append(
            make_transaction(rng, config, pop, client, tx_type, t, amount, client.home_region)
        )
    return txs


def _multiples_mediane(
    rng: random.Random, config: GeneratorConfig, pop: Population, start_ms: int, end_ms: int
) -> list[Transaction]:
    """Sorties à 6–9 × la médiane de clients éligibles à R2 : sous son facteur 10."""
    cfg = config.near_threshold
    eligibles = [c for c in pop.clients if expected_outgoing_30d(config, c) >= 5]
    txs: list[Transaction] = []
    for t in poisson_instants(rng, [(start_ms, end_ms)], cfg.multiple_mediane_per_day / DAY_MS):
        client = rng.choice(eligibles)
        factor = rng.uniform(cfg.multiple_mediane_factor_min, cfg.multiple_mediane_factor_max)
        amount = clamp_amount(config, client.median_amount_xof * factor)
        tx_type = outgoing_type(rng, config)
        txs.append(
            make_transaction(rng, config, pop, client, tx_type, t, amount, client.home_region)
        )
    return txs


def generate_near_threshold(
    config: GeneratorConfig,
    population: Population,
    sim_start: datetime,
    sim_end: datetime,
    seed: int,
) -> list[Transaction]:
    """Tout le trafic près des seuils du run, trié par ``event_time`` (H10)."""
    cfg = config.near_threshold
    start_ms, end_ms = to_ms(sim_start), to_ms(sim_end)

    def rng(kind: str) -> random.Random:
        return random.Random(f"{seed}:near_threshold:{kind}")

    jour = local_windows(
        start_ms,
        end_ms,
        config.tz,
        cfg.gros_montant_jour_hour_start,
        cfg.gros_montant_jour_hour_end,
    )
    nuit = local_windows(
        start_ms, end_ms, config.tz, cfg.nuit_sous_seuil_hour_start, cfg.nuit_sous_seuil_hour_end
    )
    heures_jour = cfg.gros_montant_jour_hour_end - cfg.gros_montant_jour_hour_start
    heures_nuit = cfg.nuit_sous_seuil_hour_end - cfg.nuit_sous_seuil_hour_start
    txs = [
        *_presque_rafales(rng("PRESQUE_RAFALE"), config, population, start_ms, end_ms),
        *_montants_fixes(
            rng("GROS_MONTANT_JOUR"),
            config,
            population,
            jour,
            cfg.gros_montant_jour_per_day / (heures_jour * HOUR_MS),
            cfg.gros_montant_jour_min_xof,
            cfg.gros_montant_jour_max_xof,
        ),
        *_montants_fixes(
            rng("NUIT_SOUS_SEUIL"),
            config,
            population,
            nuit,
            cfg.nuit_sous_seuil_per_day / (heures_nuit * HOUR_MS),
            cfg.nuit_sous_seuil_min_xof,
            cfg.nuit_sous_seuil_max_xof,
        ),
        *_multiples_mediane(rng("MULTIPLE_MEDIANE"), config, population, start_ms, end_ms),
    ]
    txs.sort(key=lambda tx: (tx.event_time, str(tx.transaction_id)))
    return txs
