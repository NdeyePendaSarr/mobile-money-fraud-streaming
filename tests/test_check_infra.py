"""Tests de la comparaison de scripts/check_infra.py, sans Kafka ni PostgreSQL."""

from __future__ import annotations

from scripts.check_infra import (
    SEPT_JOURS_MS,
    TABLES_ATTENDUES,
    TOPICS_ATTENDUS,
    PortAttendu,
    TopicObserve,
    comparer_ports,
    comparer_tables,
    comparer_topics,
)

PORTS = (
    PortAttendu("postgres", port_conteneur=5432, port_hote=5434),
    PortAttendu("kafka", port_conteneur=9092, port_hote=9092),
)


def conformes() -> dict[str, TopicObserve]:
    """Topics tels que kafka-init doit les créer."""
    return {
        "transactions": TopicObserve(partitions=3, replication=1, retention_ms=SEPT_JOURS_MS),
        "dead_letter": TopicObserve(partitions=1, replication=1, retention_ms=SEPT_JOURS_MS),
    }


def test_topics_conformes_sans_ecart() -> None:
    assert comparer_topics(TOPICS_ATTENDUS, conformes()) == []


def test_topics_supplementaires_ignores() -> None:
    observes = conformes() | {
        "__consumer_offsets": TopicObserve(partitions=50, replication=1, retention_ms=None)
    }
    assert comparer_topics(TOPICS_ATTENDUS, observes) == []


def test_topic_absent() -> None:
    observes = conformes()
    del observes["dead_letter"]
    assert comparer_topics(TOPICS_ATTENDUS, observes) == ["topic dead_letter absent"]


def test_mauvais_nombre_de_partitions() -> None:
    # Cas typique : topic créé avec la valeur par défaut du broker (1 partition)
    observes = conformes() | {
        "transactions": TopicObserve(partitions=1, replication=1, retention_ms=SEPT_JOURS_MS)
    }
    assert comparer_topics(TOPICS_ATTENDUS, observes) == [
        "topic transactions : 1 partition(s), 3 attendue(s)"
    ]


def test_retention_illisible_ou_differente() -> None:
    observes = {
        "transactions": TopicObserve(partitions=3, replication=1, retention_ms=None),
        "dead_letter": TopicObserve(partitions=1, replication=1, retention_ms=86_400_000),
    }
    ecarts = comparer_topics(TOPICS_ATTENDUS, observes)
    assert len(ecarts) == 2
    assert all("retention.ms" in ecart for ecart in ecarts)


def test_huit_tables_attendues() -> None:
    assert len(TABLES_ATTENDUES) == 8


def test_tables_presentes_et_supplementaires() -> None:
    assert comparer_tables(TABLES_ATTENDUES, set(TABLES_ATTENDUES) | {"autre"}) == []


def test_tables_absentes_triees() -> None:
    observees = set(TABLES_ATTENDUES) - {"alerts", "daily_kpi"}
    assert comparer_tables(TABLES_ATTENDUES, observees) == [
        "table alerts absente",
        "table daily_kpi absente",
    ]


def test_ports_conformes() -> None:
    observes = {"postgres": [("127.0.0.1", 5434)], "kafka": [("127.0.0.1", 9092)]}
    assert comparer_ports(PORTS, observes) == []


def test_port_ecrase_par_le_shell() -> None:
    # Cas vécu : POSTGRES_PORT=5433 exporté pour le projet 1 a écrasé le .env
    observes = {"postgres": [("127.0.0.1", 5433)], "kafka": [("127.0.0.1", 9092)]}
    assert comparer_ports(PORTS, observes) == [
        "service postgres : port 5432 publié sur 127.0.0.1:5433, 127.0.0.1:5434 attendu"
    ]


def test_port_expose_sur_toutes_les_interfaces() -> None:
    observes = {
        "postgres": [("0.0.0.0", 5434), ("::", 5434)],
        "kafka": [("127.0.0.1", 9092)],
    }
    assert comparer_ports(PORTS, observes) == [
        "service postgres : port 5432 publié sur 0.0.0.0:5434, :::5434, 127.0.0.1:5434 attendu"
    ]


def test_liaison_en_trop_avec_la_bonne() -> None:
    observes = {
        "postgres": [("127.0.0.1", 5434), ("0.0.0.0", 5434)],
        "kafka": [("127.0.0.1", 9092)],
    }
    assert len(comparer_ports(PORTS, observes)) == 1


def test_service_arrete() -> None:
    ecarts = comparer_ports(PORTS, {"postgres": [("127.0.0.1", 5434)]})
    assert ecarts == [
        "service kafka : port 9092 non publié, 127.0.0.1:9092 attendu (conteneur arrêté ?)"
    ]
