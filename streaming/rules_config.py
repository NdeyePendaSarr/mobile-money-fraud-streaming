"""Chargement de ``config/rules.toml`` (phase 4, J1).

L'image Spark est en Python 3.10, qui n'a pas ``tomllib`` (3.11+) : on prend
``tomllib`` s'il existe, sinon ``tomli`` (même API, épinglé dans
``streaming/Dockerfile``). Toute clé manquante ou inconnue, toute valeur invalide
arrête le job **au démarrage**, avec un message qui nomme la clé.
"""

from __future__ import annotations

import math
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if sys.version_info >= (3, 11):
    import tomllib
else:  # Python 3.10 de l'image Spark
    import tomli as tomllib

DEFAULT_RULES_PATH = Path("config/rules.toml")


class RulesConfigError(ValueError):
    """Configuration des règles invalide."""


@dataclass(frozen=True)
class R2Config:
    factor: float  # montant > factor × médiane (strict)
    floor_xof: int  # montant ≥ plancher (inclusif)
    min_history: int  # nb_tx_30j ≥ min_history


@dataclass(frozen=True)
class R3Config:
    amount_threshold_xof: int  # montant > seuil (strict)
    hour_start: int  # heure locale incluse
    hour_end: int  # heure locale exclue
    timezone: str  # fuseau métier (F9)


@dataclass(frozen=True)
class RulesConfig:
    r2: R2Config
    r3: R3Config


def _table(data: Mapping[str, Any], name: str, keys: set[str]) -> Mapping[str, Any]:
    table = data.get(name)
    if not isinstance(table, Mapping):
        raise RulesConfigError(f"section [{name}] absente ou invalide")
    absentes, inconnues = keys - table.keys(), table.keys() - keys
    if absentes:
        raise RulesConfigError(f"[{name}] : clé(s) manquante(s) : {', '.join(sorted(absentes))}")
    if inconnues:
        raise RulesConfigError(f"[{name}] : clé(s) inconnue(s) : {', '.join(sorted(inconnues))}")
    return table


def _int(table: Mapping[str, Any], key: str, name: str, low: int, high: int) -> int:
    value = table[key]
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise RulesConfigError(
            f"[{name}] {key} : entier entre {low} et {high} attendu, {value!r} lu"
        )
    return value


def parse_rules_config(data: Mapping[str, Any]) -> RulesConfig:
    """Valide un dictionnaire TOML déjà lu."""
    if set(data) != {"r2", "r3"}:
        raise RulesConfigError("sections attendues : [r2] et [r3] (et rien d'autre)")

    t2 = _table(data, "r2", {"factor", "floor_xof", "min_history"})
    factor = t2["factor"]
    if isinstance(factor, bool) or not isinstance(factor, int | float):
        raise RulesConfigError(f"[r2] factor : nombre attendu, {factor!r} lu")
    if not math.isfinite(float(factor)) or float(factor) <= 1:
        raise RulesConfigError(f"[r2] factor : nombre > 1 attendu, {factor!r} lu")
    r2 = R2Config(
        factor=float(factor),
        floor_xof=_int(t2, "floor_xof", "r2", 0, 2_000_000),
        min_history=_int(t2, "min_history", "r2", 0, 1_000_000),
    )

    t3 = _table(data, "r3", {"amount_threshold_xof", "hour_start", "hour_end", "timezone"})
    timezone = t3["timezone"]
    try:
        ZoneInfo(str(timezone))
    except (ZoneInfoNotFoundError, ValueError) as erreur:
        raise RulesConfigError(f"[r3] timezone inconnu : {timezone!r}") from erreur
    r3 = R3Config(
        amount_threshold_xof=_int(t3, "amount_threshold_xof", "r3", 0, 2_000_000),
        hour_start=_int(t3, "hour_start", "r3", 0, 23),
        hour_end=_int(t3, "hour_end", "r3", 1, 24),
        timezone=str(timezone),
    )
    if r3.hour_start >= r3.hour_end:
        raise RulesConfigError("[r3] : hour_start < hour_end attendu")
    return RulesConfig(r2=r2, r3=r3)


def load_rules_config(path: Path = DEFAULT_RULES_PATH) -> RulesConfig:
    """Lit et valide le fichier ; lève ``RulesConfigError`` au moindre problème."""
    try:
        with path.open("rb") as fichier:
            data = tomllib.load(fichier)
    except (OSError, tomllib.TOMLDecodeError) as erreur:
        raise RulesConfigError(f"{path} : {erreur}") from erreur
    return parse_rules_config(data)
