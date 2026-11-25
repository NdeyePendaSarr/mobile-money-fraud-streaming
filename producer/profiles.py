"""Population fictive (H7) et profils d'amorçage (H9).

La population ne dépend que de la graine et de ``[population]`` : un même run,
une même graine, donnent les mêmes clients, quels que soient la plage simulée,
la vitesse ou le nombre d'épisodes (flux aléatoire « population », H4).

Chaque client a une loi de montants sortants log-normale (mu, sigma) : sa
médiane vaut exactement ``exp(mu)``. Le profil d'amorçage la reprend telle
quelle (``round(exp(mu))``) : il est cohérent avec le trafic par construction.
"""

from __future__ import annotations

import math
import random
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import accumulate

from producer.config import GeneratorConfig
from producer.events import OUTGOING_TYPES, REGIONS


@dataclass(frozen=True)
class Client:
    user_id: str  # U-000001
    home_region: str
    mu: float  # paramètre de la loi log-normale des montants sortants
    activity: float  # part du trafic (somme sur tous les clients = 1)
    first_seen_days: float  # ancienneté au début du run, en jours (H9)

    @property
    def median_amount_xof(self) -> int:
        """Médiane exacte de la loi des montants sortants."""
        return round(math.exp(self.mu))


@dataclass(frozen=True)
class Profile:
    """Ligne de ``user_profiles`` (source GENERATEUR)."""

    user_id: str
    median_amount_xof: int
    nb_tx_30j: int
    home_region: str
    first_seen: datetime


class Population:
    """Clients, agents (par région) et marchands, avec tirage pondéré par l'activité."""

    def __init__(self, clients: list[Client], agents: dict[str, list[str]], merchants: list[str]):
        self.clients = clients
        self.agents_by_region = agents
        self.merchants = merchants
        # Bornes cumulées pour un tirage pondéré en O(log n)
        self._cumulative = list(accumulate(c.activity for c in clients))

    def pick_client(self, rng: random.Random) -> Client:
        """Tire un client au prorata de son activité."""
        index = bisect_right(self._cumulative, rng.random() * self._cumulative[-1])
        return self.clients[min(index, len(self.clients) - 1)]


def outgoing_share(config: GeneratorConfig) -> float:
    """Part des transactions sortantes (TRANSFER, PAYMENT, CASH_OUT) dans le mix."""
    weights = config.traffic.type_weights
    return sum(weights.get(t, 0.0) for t in OUTGOING_TYPES) / sum(weights.values())


def expected_outgoing_30d(config: GeneratorConfig, client: Client) -> int:
    """Espérance arrondie du nombre de sorties du client sur 30 jours (``nb_tx_30j``)."""
    return round(30 * config.traffic.tx_per_day * outgoing_share(config) * client.activity)


def build_population(config: GeneratorConfig, seed: int) -> Population:
    """Construit la population à partir du flux aléatoire « population »."""
    rng = random.Random(f"{seed}:population")
    pop = config.population
    regions = list(pop.region_weights)
    region_weights = [pop.region_weights[r] for r in regions]
    amounts = config.amounts
    # mu borné pour que la médiane reste dans les bornes de montant
    mu_min, mu_max = math.log(amounts.min_xof), math.log(amounts.max_xof)

    raw_activity = [rng.lognormvariate(0.0, pop.activity_sigma) for _ in range(pop.n_clients)]
    total_activity = sum(raw_activity)
    clients: list[Client] = []
    for i, raw in enumerate(raw_activity, start=1):
        mu = min(max(rng.gauss(pop.amount_mu_mean, pop.amount_mu_sd), mu_min), mu_max)
        clients.append(
            Client(
                user_id=f"U-{i:06d}",
                home_region=rng.choices(regions, region_weights)[0],
                mu=mu,
                activity=raw / total_activity,
                first_seen_days=rng.uniform(pop.first_seen_days_min, pop.first_seen_days_max),
            )
        )

    # Agents : au moins un par région (un CASH_IN doit toujours trouver un agent
    # local), les autres répartis selon les poids des régions
    agents: dict[str, list[str]] = {r: [] for r in REGIONS}
    for i in range(1, pop.n_agents + 1):
        region = REGIONS[i - 1] if i <= len(REGIONS) else rng.choices(regions, region_weights)[0]
        agents[region].append(f"A-{i:04d}")

    merchants = [f"M-{i:04d}" for i in range(1, pop.n_merchants + 1)]
    return Population(clients, agents, merchants)


def bootstrap_profiles(
    config: GeneratorConfig, population: Population, sim_start: datetime
) -> list[Profile]:
    """Profils d'amorçage (H9) : l'« historique de l'ancien système ».

    - médiane = ``round(exp(mu))``, médiane exacte de la loi du client ;
    - ``nb_tx_30j`` = espérance arrondie de ses sorties sur 30 jours ;
    - ``first_seen`` = ``sim_start`` − ancienneté tirée dans la population.
    """
    return [
        Profile(
            user_id=c.user_id,
            median_amount_xof=c.median_amount_xof,
            nb_tx_30j=expected_outgoing_30d(config, c),
            home_region=c.home_region,
            first_seen=sim_start - timedelta(days=c.first_seen_days),
        )
        for c in population.clients
    ]
