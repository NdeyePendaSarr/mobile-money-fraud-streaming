"""Test n° 9 (intégration) : un run réel d'une heure simulée en --speed max.

Exige Kafka et PostgreSQL démarrés (``make up``) ; lancé par
``make test-integration``, jamais par ``make check``. Il **écrit un run en base**
et des messages dans le topic : faire ``make reset`` avant un run de référence.

Le nombre de messages est mesuré par différence des offsets de fin, avant et
après le run : le test ne suppose pas un topic vide.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from confluent_kafka import Consumer, TopicPartition

from producer.events import format_event_time
from producer.producer import TOPIC, main

pytestmark = pytest.mark.integration


def _connect() -> psycopg.Connection[tuple[Any, ...]]:
    return psycopg.connect(
        host=os.environ["SENPAY_PG_HOST"],
        port=int(os.environ["SENPAY_PG_PORT"]),
        user=os.environ["SENPAY_PG_USER"],
        password=os.environ["SENPAY_PG_PASSWORD"],
        dbname=os.environ["SENPAY_PG_DB"],
    )


def _end_offsets_total() -> int:
    """Somme des offsets de fin des 3 partitions de ``transactions``."""
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["SENPAY_KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": "test-integration-offsets",
            "enable.auto.commit": False,
        }
    )
    try:
        partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        return sum(
            consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1]
            for p in partitions
        )
    finally:
        consumer.close()


def test_run_d_une_heure_en_speed_max() -> None:
    reel = datetime.now(UTC)  # heure réelle injectée, comme au point d'entrée
    with _connect() as db:
        row = db.execute(
            "select max(sim_end), count(*) filter (where ended_at is null) from generator_runs"
        ).fetchone()
    assert row is not None
    dernier, inacheves = row
    if inacheves:
        pytest.skip("run inachevé en base : make reset")
    # Plage d'une heure, après le dernier run et au moins 1 h dans le passé
    debut = (reel - timedelta(hours=3)).replace(minute=0, second=0, microsecond=0)
    if dernier is not None and dernier > debut:
        debut = dernier
    if debut + timedelta(hours=1) > reel:
        pytest.skip("plus de plage libre avant l'heure réelle : make reset")

    avant = _end_offsets_total()
    code = main(
        [
            "--seed",
            "4242",
            "--start",
            format_event_time(debut),
            "--duration",
            "1h",
            "--speed",
            "max",
        ],
        now=lambda: datetime.now(UTC),
    )
    assert code == 0
    apres = _end_offsets_total()

    with _connect() as db:
        run = db.execute(
            "select run_id, nb_sent, nb_episodes, ended_at, sim_end "
            "from generator_runs order by run_id desc limit 1"
        ).fetchone()
        assert run is not None
        run_id, nb_sent, nb_episodes, ended_at, sim_end = run
        par_type = dict(
            db.execute(
                "select anomaly_type, count(distinct anomaly_id) from injected_anomalies "
                "where run_id = %s group by anomaly_type",
                (run_id,),
            ).fetchall()
        )

    assert ended_at is not None
    assert sim_end == debut + timedelta(hours=1)
    assert nb_sent > 0
    assert apres - avant == nb_sent
    assert par_type == {t: n for t, n in nb_episodes.items() if n > 0}
