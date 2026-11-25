"""Fixtures partagées : un jour simulé (graine 42), calculé une seule fois."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from producer.config import GeneratorConfig, load_config
from producer.events import Emission
from producer.producer import Plan, plan_run

SEED = 42
SIM_START = datetime(2026, 11, 1, tzinfo=UTC)
SIM_END = SIM_START + timedelta(days=1)


@pytest.fixture(scope="session")
def config() -> GeneratorConfig:
    return load_config()


@pytest.fixture(scope="session")
def day_plan(config: GeneratorConfig) -> Plan:
    return plan_run(config, SEED, SIM_START, SIM_END)


@pytest.fixture(scope="session")
def day_emissions(day_plan: Plan) -> list[Emission]:
    """Toutes les émissions du jour, dans l'ordre d'envoi."""
    return [e for emissions in day_plan.slices() for e in emissions]
