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
from dataclasses import asdict, dataclass, fields
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
class CashOutRapideConfig:
    per_day: float
    out_ratio_min: float
    out_ratio_max: float
    delay_min_s: int
    delay_max_s: int


@dataclass(frozen=True)
class ChangementRegionConfig:
    per_day: float
    delay_min_s: int
    delay_max_s: int
    far_pairs: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class EpisodesConfig:
    rafale: RafaleConfig
    montant_anormal: MontantAnormalConfig
    nuit_gros_montant: NuitGrosMontantConfig
    cash_out_rapide: CashOutRapideConfig
    changement_region: ChangementRegionConfig


@dataclass(frozen=True)
class NearThresholdConfig:
    """Trafic normal près des seuils (H10) ; absent de la vérité terrain."""

    presque_rafale_per_day: float
    presque_rafale_transfers: int
    presque_rafale_max_duration_s: int
    gros_montant_jour_per_day: float
    gros_montant_jour_min_xof: int
    gros_montant_jour_max_xof: int
    gros_montant_jour_hour_start: int
    gros_montant_jour_hour_end: int
    nuit_sous_seuil_per_day: float
    nuit_sous_seuil_min_xof: int
    nuit_sous_seuil_max_xof: int
    nuit_sous_seuil_hour_start: int
    nuit_sous_seuil_hour_end: int
    multiple_mediane_per_day: float
    multiple_mediane_factor_min: float
    multiple_mediane_factor_max: float


@dataclass(frozen=True)
class DelaysConfig:
    """Retards d'émission (H14), en parts des messages et en secondes simulées."""

    late_share: float
    late_min_s: int
    late_max_s: int
    very_late_share: float
    very_late_min_s: int
    very_late_max_s: int


@dataclass(frozen=True)
class InvalidConfig:
    """Messages invalides (H15)."""

    share: float
    future_offset_min_s: int
    future_offset_max_s: int


@dataclass(frozen=True)
class GeneratorConfig:
    timezone: str
    amounts: AmountsConfig
    population: PopulationConfig
    traffic: TrafficConfig
    episodes: EpisodesConfig
    near_threshold: NearThresholdConfig
    delays: DelaysConfig
    invalid: InvalidConfig

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


def _hours(
    table: Mapping[str, Any], name: str, start_key: str = "hour_start", end_key: str = "hour_end"
) -> tuple[int, int]:
    """Plage horaire locale [start ; end[, avec 0 ≤ start < end ≤ 24."""
    start = _int(table, start_key, name, 0)
    end = _int(table, end_key, name, 1)
    if not start < end <= 24:
        raise ConfigError(f"[{name}] : 0 ≤ {start_key} < {end_key} ≤ 24 attendu")
    return start, end


def _ordered(name: str, low: float, high: float, what: str) -> None:
    """Refuse une borne basse au-dessus de la borne haute."""
    if low > high:
        raise ConfigError(f"[{name}] {what} : borne basse ≤ borne haute attendue")


def _share(table: Mapping[str, Any], key: str, name: str) -> float:
    """Part entre 0 et 1."""
    value = _float(table, key, name)
    if value > 1:
        raise ConfigError(f"[{name}] {key} : part entre 0 et 1 attendue")
    return value


# --- Construction -----------------------------------------------------------


def parse_config(data: Mapping[str, Any]) -> GeneratorConfig:
    """Valide un dictionnaire TOML déjà lu et construit la configuration."""
    racine = {
        "timezone",
        "amounts",
        "population",
        "traffic",
        "episodes",
        "near_threshold",
        "delays",
        "invalid",
    }
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
    sous_sections = {
        "rafale",
        "montant_anormal",
        "nuit_gros_montant",
        "cash_out_rapide",
        "changement_region",
    }
    if not isinstance(ep, Mapping) or set(ep) != sous_sections:
        raise ConfigError(
            f"[episodes] : sous-sections attendues : {', '.join(sorted(sous_sections))}"
        )

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

    name = "episodes.cash_out_rapide"
    c = _table(
        ep,
        "cash_out_rapide",
        {"per_day", "out_ratio_min", "out_ratio_max", "delay_min_s", "delay_max_s"},
    )
    cash_out = CashOutRapideConfig(
        per_day=_float(c, "per_day", name),
        out_ratio_min=_share(c, "out_ratio_min", name),
        out_ratio_max=_share(c, "out_ratio_max", name),
        delay_min_s=_int(c, "delay_min_s", name, 1),
        delay_max_s=_int(c, "delay_max_s", name, 1),
    )
    _ordered(name, cash_out.out_ratio_min, cash_out.out_ratio_max, "out_ratio")
    _ordered(name, cash_out.delay_min_s, cash_out.delay_max_s, "delay")

    name = "episodes.changement_region"
    g = _table(ep, "changement_region", {"per_day", "delay_min_s", "delay_max_s", "far_pairs"})
    raw_pairs = g["far_pairs"]
    if not isinstance(raw_pairs, list) or not raw_pairs:
        raise ConfigError(f"[{name}] far_pairs : liste non vide de paires attendue")
    pairs: list[tuple[str, str]] = []
    for pair in raw_pairs:
        if (
            not isinstance(pair, list)
            or len(pair) != 2
            or not all(r in REGIONS for r in pair)
            or pair[0] == pair[1]
        ):
            raise ConfigError(f"[{name}] far_pairs : paire de deux régions distinctes : {pair!r}")
        pairs.append((pair[0], pair[1]))
    changement = ChangementRegionConfig(
        per_day=_float(g, "per_day", name),
        delay_min_s=_int(g, "delay_min_s", name, 1),
        delay_max_s=_int(g, "delay_max_s", name, 1),
        far_pairs=tuple(pairs),
    )
    _ordered(name, changement.delay_min_s, changement.delay_max_s, "delay")

    name = "near_threshold"
    cles = {f.name for f in fields(NearThresholdConfig)}
    s = _table(data, name, cles)
    pr_start, pr_end = _hours(s, name, "gros_montant_jour_hour_start", "gros_montant_jour_hour_end")
    ns_start, ns_end = _hours(s, name, "nuit_sous_seuil_hour_start", "nuit_sous_seuil_hour_end")
    near = NearThresholdConfig(
        presque_rafale_per_day=_float(s, "presque_rafale_per_day", name),
        presque_rafale_transfers=_int(s, "presque_rafale_transfers", name, 2),
        presque_rafale_max_duration_s=_int(s, "presque_rafale_max_duration_s", name, 1),
        gros_montant_jour_per_day=_float(s, "gros_montant_jour_per_day", name),
        gros_montant_jour_min_xof=_int(s, "gros_montant_jour_min_xof", name, amounts.min_xof),
        gros_montant_jour_max_xof=_int(s, "gros_montant_jour_max_xof", name, amounts.min_xof),
        gros_montant_jour_hour_start=pr_start,
        gros_montant_jour_hour_end=pr_end,
        nuit_sous_seuil_per_day=_float(s, "nuit_sous_seuil_per_day", name),
        nuit_sous_seuil_min_xof=_int(s, "nuit_sous_seuil_min_xof", name, amounts.min_xof),
        nuit_sous_seuil_max_xof=_int(s, "nuit_sous_seuil_max_xof", name, amounts.min_xof),
        nuit_sous_seuil_hour_start=ns_start,
        nuit_sous_seuil_hour_end=ns_end,
        multiple_mediane_per_day=_float(s, "multiple_mediane_per_day", name),
        multiple_mediane_factor_min=_float(s, "multiple_mediane_factor_min", name, 1.0),
        multiple_mediane_factor_max=_float(s, "multiple_mediane_factor_max", name, 1.0),
    )
    _ordered(
        name, near.gros_montant_jour_min_xof, near.gros_montant_jour_max_xof, "gros_montant_jour"
    )
    _ordered(name, near.nuit_sous_seuil_min_xof, near.nuit_sous_seuil_max_xof, "nuit_sous_seuil")
    _ordered(name, near.multiple_mediane_factor_min, near.multiple_mediane_factor_max, "facteur")
    if max(near.gros_montant_jour_max_xof, near.nuit_sous_seuil_max_xof) > amounts.max_xof:
        raise ConfigError(f"[{name}] : montants ≤ amounts.max_xof attendus")

    name = "delays"
    d = _table(data, name, {f.name for f in fields(DelaysConfig)})
    delays = DelaysConfig(
        late_share=_share(d, "late_share", name),
        late_min_s=_int(d, "late_min_s", name, 1),
        late_max_s=_int(d, "late_max_s", name, 1),
        very_late_share=_share(d, "very_late_share", name),
        very_late_min_s=_int(d, "very_late_min_s", name, 1),
        very_late_max_s=_int(d, "very_late_max_s", name, 1),
    )
    _ordered(name, delays.late_min_s, delays.late_max_s, "late")
    _ordered(name, delays.very_late_min_s, delays.very_late_max_s, "very_late")
    if delays.late_share + delays.very_late_share > 1:
        raise ConfigError(f"[{name}] : late_share + very_late_share ≤ 1 attendu")

    name = "invalid"
    v = _table(data, name, {f.name for f in fields(InvalidConfig)})
    invalid = InvalidConfig(
        share=_share(v, "share", name),
        # Au-delà de la tolérance de 5 min du schéma, sinon le message serait valide
        future_offset_min_s=_int(v, "future_offset_min_s", name, 301),
        future_offset_max_s=_int(v, "future_offset_max_s", name, 301),
    )
    _ordered(name, invalid.future_offset_min_s, invalid.future_offset_max_s, "future_offset")

    return GeneratorConfig(
        timezone=str(timezone),
        amounts=amounts,
        population=population,
        traffic=traffic,
        episodes=EpisodesConfig(rafale, montant, nuit, cash_out, changement),
        near_threshold=near,
        delays=delays,
        invalid=invalid,
    )


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> GeneratorConfig:
    """Lit et valide le fichier TOML."""
    try:
        with path.open("rb") as fichier:
            data = tomllib.load(fichier)
    except (OSError, tomllib.TOMLDecodeError) as erreur:
        raise ConfigError(f"{path} : {erreur}") from erreur
    return parse_config(data)
