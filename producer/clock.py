"""Horloge simulée du générateur (décisions F7, F8, H3, H5, H6).

Le temps simulé avance `speed` fois plus vite que le temps réel :
    real_deadline(t) = real_start + (t - sim_start) / speed
L'heure réelle n'est jamais lue ici : elle est passée en paramètre
(`real_start`, `real_now`), ce qui rend le module testable sans `sleep`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta

# Unités acceptées par --duration : 30min, 2h, 1d, 30d.
_DURATION_RE = re.compile(r"^(\d+)(min|h|d)$")
_UNITS: dict[str, timedelta] = {
    "min": timedelta(minutes=1),
    "h": timedelta(hours=1),
    "d": timedelta(days=1),
}


def _require_aware(name: str, value: datetime) -> None:
    """Refuse un datetime « naïf » : on ne suppose jamais de fuseau (F9)."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} doit avoir un fuseau (UTC), reçu : {value!r}")


def parse_duration(text: str) -> timedelta:
    """Convertit '30min', '2h', '1d' en timedelta strictement positif.

    Raises:
        ValueError: format inconnu ('1w', '-1h', 'abc', '') ou durée nulle ('0h').
    """
    match = _DURATION_RE.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"durée invalide : {text!r} (attendu : 30min, 2h, 1d…)")
    amount, unit = int(match.group(1)), match.group(2)
    if amount == 0:
        raise ValueError(f"durée nulle : {text!r}")
    return amount * _UNITS[unit]


@dataclass(frozen=True)
class SimClock:
    """Correspondance temps simulé → temps réel pour un run.

    Attributes:
        sim_start: début de la plage simulée (UTC, avec fuseau).
        speed: facteur de vitesse (60 = 1 h simulée en 1 min réelle) ;
            None = --speed max, aucune attente (H6).
        real_start: heure réelle du lancement du run (UTC, avec fuseau).
    """

    sim_start: datetime
    speed: float | None
    real_start: datetime

    def __post_init__(self) -> None:
        _require_aware("sim_start", self.sim_start)
        _require_aware("real_start", self.real_start)
        if self.speed is not None and self.speed <= 0:
            raise ValueError(f"speed doit être > 0, reçu : {self.speed}")

    def real_deadline(self, sim_t: datetime) -> datetime | None:
        """Heure réelle à laquelle émettre l'événement simulé `sim_t`.

        Renvoie None en --speed max : le producteur n'attend pas.
        """
        _require_aware("sim_t", sim_t)
        if self.speed is None:
            return None
        return self.real_start + (sim_t - self.sim_start) / self.speed


def check_range(
    sim_start: datetime,
    sim_end: datetime,
    real_now: datetime,
    last_sim_end: datetime | None,
) -> None:
    """Vérifie qu'une plage simulée est jouable (F7, F8, H5).

    Règles :
    - sim_start < sim_end ;
    - sim_end <= real_now : l'horloge simulée ne dépasse jamais l'heure réelle ;
    - sim_start >= last_sim_end : pas de rejeu dans le passé sans `make reset`
      (le watermark ignorerait tout). Des runs contigus sont acceptés.

    Raises:
        ValueError: avec un message qui dit quoi faire.
    """
    for name, value in (("sim_start", sim_start), ("sim_end", sim_end), ("real_now", real_now)):
        _require_aware(name, value)
    if last_sim_end is not None:
        _require_aware("last_sim_end", last_sim_end)

    if sim_end <= sim_start:
        raise ValueError(f"plage vide : sim_end ({sim_end}) <= sim_start ({sim_start})")
    if sim_end > real_now:
        raise ValueError(
            f"la plage finit dans le futur ({sim_end} > {real_now}) : "
            "choisis un --start plus ancien ou une --duration plus courte"
        )
    if last_sim_end is not None and sim_start < last_sim_end:
        raise ValueError(
            f"--start ({sim_start}) est antérieur à la fin du dernier run ({last_sim_end}) : "
            "lance `make reset` avant de rejouer une plage passée"
        )
