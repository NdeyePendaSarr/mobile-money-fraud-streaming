"""Chargement et validation de ``config/generator.toml`` (H17).

Le fichier est lu avec ``tomllib`` (bibliothèque standard) et transformé en
dataclasses figées. Toute clé inconnue ou manquante, tout poids négatif, toute
borne incohérente est refusé **au chargement**, avec un message qui nomme la clé :
un run ne démarre jamais sur une configuration douteuse.
"""

from __future__ import annotations

import math
import tomllib
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from producer.events import REGIONS, TYPES

DEFAULT_CONFIG_PATH = Path("config/generator.toml")


class ConfigError(ValueError):
    """Configuration du générateur invalide."""


@dataclass(frozen=True)
class AmountsConfig:
    min_xof: int
    max_xof: int


@dataclass(frozen=True)
class PopulationConfig:
    n_clients: int
    n_agents: int
    n_merchants: int
    region_weights: dict[str, float]
    activity_sigma: float
    amount_mu_mean: float
    amount_mu_sd: float
    amount_sigma: float
    cash_in_mu_shift: float
    first_seen_days_min: int
    first_seen_days_max: int


@dataclass(frozen=True)
class TrafficConfig:
    tx_per_day: int
    hourly_weights: tuple[float, ...]
    type_weights: dict[str, float]
    channel_weights: dict[str, float]
    home_region_share: float


@dataclass(frozen=True)
class RafaleConfig:
    per_day: float
    min_transfers: int
    max_transfers: int
    max_duration_s: int


@dataclass(frozen=True)
class MontantAnormalConfig:
    per_day: float
    factor_min: float
    factor_max: float
    floor_xof: int
    min_history: int
    hour_start: int
    hour_end: int


@dataclass(frozen=True)
class NuitGrosMontantConfig:
    per_day: float
    amount_min_xof: int
    amount_max_xof: int
    hour_start: int
    hour_end: int


@dataclass(frozen=True)
class EpisodesConfig:
    rafale: RafaleConfig
    montant_anormal: MontantAnormalConfig
    nuit_gros_montant: NuitGrosMontantConfig


@dataclass(frozen=True)
class GeneratorConfig:
    timezone: str
    amounts: AmountsConfig
    population: PopulationConfig
    traffic: TrafficConfig
    episodes: EpisodesConfig

    @property
    def tz(self) -> ZoneInfo:
        """Fuseau métier (F9)."""
        return ZoneInfo(self.timezone)

    def as_dict(self) -> dict[str, Any]:
        """Configuration complète, sérialisable en JSON (``generator_runs.params``)."""
        return asdict(self)


# --- Lecture stricte des sections -------------------------------------------


def _table(data: Mapping[str, Any], name: str, keys: set[str]) -> Mapping[str, Any]:
    """Renvoie la sous-table ``name`` en exigeant exactement les clés ``keys``."""
    table = data.get(name)
    if not isinstance(table, Mapping):
        raise ConfigError(f"section [{name}] absente ou invalide")
    absentes = keys - table.keys()
    inconnues = table.keys() - keys
    if absentes:
        raise ConfigError(f"[{name}] : clé(s) manquante(s) : {', '.join(sorted(absentes))}")
    if inconnues:
        raise ConfigError(f"[{name}] : clé(s) inconnue(s) : {', '.join(sorted(inconnues))}")
    return table


def _int(table: Mapping[str, Any], key: str, name: str, minimum: int) -> int:
    value = table[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ConfigError(f"[{name}] {key} : entier ≥ {minimum} attendu, {value!r} lu")
    return value


def _float(table: Mapping[str, Any], key: str, name: str, minimum: float = 0.0) -> float:
    value = table[key]
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigError(f"[{name}] {key} : nombre attendu, {value!r} lu")
    number = float(value)
    if not math.isfinite(number) or number < minimum:
        raise ConfigError(f"[{name}] {key} : nombre ≥ {minimum} attendu, {value!r} lu")
    return number


def _weights(
    table: Mapping[str, Any], key: str, name: str, allowed: tuple[str, ...]
) -> dict[str, float]:
    """Poids relatifs ≥ 0, clés dans ``allowed``, somme > 0. Ordre de ``allowed`` conservé."""
    raw = table[key]
    if not isinstance(raw, Mapping):
        raise ConfigError(f"[{name}] {key} : table de poids attendue")
    inconnues = set(raw) - set(allowed)
    if inconnues:
        raise ConfigError(f"[{name}] {key} : clé(s) inconnue(s) : {', '.join(sorted(inconnues))}")
    weights = {k: _float(raw, k, f"{name}.{key}") for k in allowed if k in raw}
    if sum(weights.values()) <= 0:
        raise ConfigError(f"[{name}] {key} : la somme des poids doit être > 0")
    return weights


def _hours(table: Mapping[str, Any], name: str) -> tuple[int, int]:
    start = _int(table, "hour_start", name, 0)
    end = _int(table, "hour_end", name, 1)
    if not start < end <= 24:
        raise ConfigError(f"[{name}] : 0 ≤ hour_start < hour_end ≤ 24 attendu")
    return start, end


# --- Construction -----------------------------------------------------------


def parse_config(data: Mapping[str, Any]) -> GeneratorConfig:
    """Valide un dictionnaire TOML déjà lu et construit la configuration."""
    racine = {"timezone", "amounts", "population", "traffic", "episodes"}
    if set(data) != racine:
        raise ConfigError(f"clés de premier niveau attendues : {', '.join(sorted(racine))}")
    timezone = data["timezone"]
    try:
        ZoneInfo(str(timezone))
    except (ZoneInfoNotFoundError, ValueError) as erreur:
        raise ConfigError(f"timezone inconnu : {timezone!r}") from erreur

    t = _table(data, "amounts", {"min_xof", "max_xof"})
    amounts = AmountsConfig(_int(t, "min_xof", "amounts", 1), _int(t, "max_xof", "amounts", 1))
    if amounts.min_xof >= amounts.max_xof:
        raise ConfigError("[amounts] : min_xof < max_xof attendu")

    p = _table(
        data,
        "population",
        {
            "n_clients",
            "n_agents",
            "n_merchants",
            "region_weights",
            "activity_sigma",
            "amount_mu_mean",
            "amount_mu_sd",
            "amount_sigma",
            "cash_in_mu_shift",
            "first_seen_days_min",
            "first_seen_days_max",
        },
    )
    population = PopulationConfig(
        # Formats d'identifiants : U-000000 (6 chiffres), A-0000 et M-0000 (4 chiffres)
        n_clients=_int(p, "n_clients", "population", 11),
        n_agents=_int(p, "n_agents", "population", len(REGIONS)),
        n_merchants=_int(p, "n_merchants", "population", 1),
        region_weights=_weights(p, "region_weights", "population", REGIONS),
        activity_sigma=_float(p, "activity_sigma", "population"),
        amount_mu_mean=_float(p, "amount_mu_mean", "population"),
        amount_mu_sd=_float(p, "amount_mu_sd", "population"),
        amount_sigma=_float(p, "amount_sigma", "population"),
        cash_in_mu_shift=_float(p, "cash_in_mu_shift", "population", -10.0),
        first_seen_days_min=_int(p, "first_seen_days_min", "population", 0),
        first_seen_days_max=_int(p, "first_seen_days_max", "population", 0),
    )
    if population.n_clients > 999_999:
        raise ConfigError("[population] n_clients ≤ 999 999 (format U-000000)")
    if population.n_agents > 9_999 or population.n_merchants > 9_999:
        raise ConfigError("[population] n_agents et n_merchants ≤ 9 999 (formats A-0000, M-0000)")
    if any(population.region_weights.get(r, 0.0) <= 0 for r in REGIONS):
        raise ConfigError("[population] region_weights : un poids > 0 pour chacune des 6 régions")
    if population.first_seen_days_min > population.first_seen_days_max:
        raise ConfigError("[population] first_seen_days_min ≤ first_seen_days_max attendu")

    tr = _table(
        data,
        "traffic",
        {"tx_per_day", "hourly_weights", "type_weights", "channel_weights", "home_region_share"},
    )
    hourly = tr["hourly_weights"]
    if not isinstance(hourly, list) or len(hourly) != 24:
        raise ConfigError("[traffic] hourly_weights : liste de 24 poids attendue")
    hourly_weights = tuple(
        _float({"h": h}, "h", f"traffic.hourly_weights[{i}]") for i, h in enumerate(hourly)
    )
    if sum(hourly_weights) <= 0:
        raise ConfigError("[traffic] hourly_weights : la somme des poids doit être > 0")
    traffic = TrafficConfig(
        tx_per_day=_int(tr, "tx_per_day", "traffic", 1),
        hourly_weights=hourly_weights,
        type_weights=_weights(tr, "type_weights", "traffic", TYPES),
        channel_weights=_weights(tr, "channel_weights", "traffic", ("APP", "USSD")),
        home_region_share=_float(tr, "home_region_share", "traffic"),
    )
    if traffic.home_region_share > 1:
        raise ConfigError("[traffic] home_region_share ≤ 1 attendu")

    ep = data["episodes"]
    if not isinstance(ep, Mapping) or set(ep) != {
        "rafale",
        "montant_anormal",
        "nuit_gros_montant",
    }:
        raise ConfigError("[episodes] : sous-sections rafale, montant_anormal, nuit_gros_montant")

    r = _table(ep, "rafale", {"per_day", "min_transfers", "max_transfers", "max_duration_s"})
    rafale = RafaleConfig(
        per_day=_float(r, "per_day", "episodes.rafale"),
        min_transfers=_int(r, "min_transfers", "episodes.rafale", 2),
        max_transfers=_int(r, "max_transfers", "episodes.rafale", 2),
        max_duration_s=_int(r, "max_duration_s", "episodes.rafale", 1),
    )
    if rafale.min_transfers > rafale.max_transfers:
        raise ConfigError("[episodes.rafale] min_transfers ≤ max_transfers attendu")

    m = _table(
        ep,
        "montant_anormal",
        {
            "per_day",
            "factor_min",
            "factor_max",
            "floor_xof",
            "min_history",
            "hour_start",
            "hour_end",
        },
    )
    m_start, m_end = _hours(m, "episodes.montant_anormal")
    montant = MontantAnormalConfig(
        per_day=_float(m, "per_day", "episodes.montant_anormal"),
        factor_min=_float(m, "factor_min", "episodes.montant_anormal", 1.0),
        factor_max=_float(m, "factor_max", "episodes.montant_anormal", 1.0),
        floor_xof=_int(m, "floor_xof", "episodes.montant_anormal", amounts.min_xof),
        min_history=_int(m, "min_history", "episodes.montant_anormal", 0),
        hour_start=m_start,
        hour_end=m_end,
    )
    if montant.factor_min > montant.factor_max:
        raise ConfigError("[episodes.montant_anormal] factor_min ≤ factor_max attendu")
    if montant.floor_xof > amounts.max_xof:
        raise ConfigError("[episodes.montant_anormal] floor_xof ≤ amounts.max_xof attendu")

    n = _table(
        ep,
        "nuit_gros_montant",
        {"per_day", "amount_min_xof", "amount_max_xof", "hour_start", "hour_end"},
    )
    n_start, n_end = _hours(n, "episodes.nuit_gros_montant")
    nuit = NuitGrosMontantConfig(
        per_day=_float(n, "per_day", "episodes.nuit_gros_montant"),
        amount_min_xof=_int(n, "amount_min_xof", "episodes.nuit_gros_montant", amounts.min_xof),
        amount_max_xof=_int(n, "amount_max_xof", "episodes.nuit_gros_montant", amounts.min_xof),
        hour_start=n_start,
        hour_end=n_end,
    )
    if not nuit.amount_min_xof <= nuit.amount_max_xof <= amounts.max_xof:
        raise ConfigError(
            "[episodes.nuit_gros_montant] amount_min_xof ≤ amount_max_xof ≤ amounts.max_xof"
        )

    return GeneratorConfig(
        timezone=str(timezone),
        amounts=amounts,
        population=population,
        traffic=traffic,
        episodes=EpisodesConfig(rafale, montant, nuit),
    )


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> GeneratorConfig:
    """Lit et valide le fichier TOML."""
    try:
        with path.open("rb") as fichier:
            data = tomllib.load(fichier)
    except (OSError, tomllib.TOMLDecodeError) as erreur:
        raise ConfigError(f"{path} : {erreur}") from erreur
    return parse_config(data)
