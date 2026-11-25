"""Tests de l'horloge simulée (producer/clock.py)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from producer.clock import SimClock, check_range, parse_duration

T0 = datetime(2026, 11, 20, tzinfo=UTC)
NOW = datetime(2026, 11, 25, 12, tzinfo=UTC)
MS = timedelta(milliseconds=1)


# --- parse_duration ----------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("30min", timedelta(minutes=30)),
        ("2h", timedelta(hours=2)),
        ("1d", timedelta(days=1)),
        ("30d", timedelta(days=30)),
    ],
)
def test_parse_duration_ok(text: str, expected: timedelta) -> None:
    assert parse_duration(text) == expected


@pytest.mark.parametrize("text", ["", "h", "-1h", "1w", "abc", "0h", "1.5h", "2 h"])
def test_parse_duration_refuse(text: str) -> None:
    with pytest.raises(ValueError):
        parse_duration(text)


# --- SimClock ----------------------------------------------------------------


def test_real_deadline_lineaire_a_vitesse_60() -> None:
    clock = SimClock(sim_start=T0, speed=60, real_start=NOW)
    # 60 s simulées = 1 s réelle ; 1 h simulée = 1 min réelle
    assert clock.real_deadline(T0) == NOW
    assert clock.real_deadline(T0 + timedelta(seconds=60)) == NOW + timedelta(seconds=1)
    assert clock.real_deadline(T0 + timedelta(hours=1)) == NOW + timedelta(minutes=1)


def test_speed_max_ne_cadence_pas() -> None:
    clock = SimClock(sim_start=T0, speed=None, real_start=NOW)
    assert clock.real_deadline(T0 + timedelta(hours=5)) is None


@pytest.mark.parametrize("speed", [0, -1])
def test_speed_non_positive_refusee(speed: float) -> None:
    with pytest.raises(ValueError):
        SimClock(sim_start=T0, speed=speed, real_start=NOW)


def test_datetime_naif_refuse() -> None:
    naive = datetime(2026, 11, 20)
    with pytest.raises(ValueError):
        SimClock(sim_start=naive, speed=60, real_start=NOW)
    clock = SimClock(sim_start=T0, speed=60, real_start=NOW)
    with pytest.raises(ValueError):
        clock.real_deadline(naive)


# --- check_range -------------------------------------------------------------


def test_plage_finissant_a_l_heure_reelle_acceptee() -> None:
    check_range(T0, NOW, NOW, None)  # borne incluse


def test_plage_depassant_l_heure_reelle_de_1_ms_refusee() -> None:
    with pytest.raises(ValueError, match="futur"):
        check_range(T0, NOW + MS, NOW, None)


def test_runs_contigus_acceptes() -> None:
    check_range(T0, T0 + timedelta(days=1), NOW, last_sim_end=T0)


def test_rejeu_dans_le_passe_refuse() -> None:
    with pytest.raises(ValueError, match="make reset"):
        check_range(T0 - MS, T0 + timedelta(days=1), NOW, last_sim_end=T0)


def test_plage_vide_refusee() -> None:
    with pytest.raises(ValueError, match="vide"):
        check_range(T0, T0, NOW, None)


def test_check_range_refuse_un_datetime_naif() -> None:
    with pytest.raises(ValueError):
        check_range(datetime(2026, 11, 20), NOW, NOW, None)
