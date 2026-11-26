"""Session Spark locale pour les tests (sur l'hôte : pyspark 4.2.1 + JDK 17)."""

from __future__ import annotations

import shutil
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Une session pour tous les tests : mêmes réglages de fuseau que le job (F9).

    Sans Java, les tests échouent (au lieu d'être sautés) : un test de parité
    sauté en silence laisserait make check vert sans rien prouver.
    """
    if shutil.which("java") is None:
        pytest.fail("Java introuvable : installer openjdk-17-jdk-headless (tests Spark sur l'hôte)")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("senpay-tests")
        .config("spark.sql.session.timeZone", "Africa/Dakar")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
