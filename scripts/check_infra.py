"""Vérifie le socle de la phase 1 : topics Kafka et tables PostgreSQL.

Contrôles :
- les ports de PostgreSQL et de Kafka sont publiés sur 127.0.0.1, au port attendu
  (celui de ``.env``), et nulle part ailleurs ;
- les topics ``transactions`` et ``dead_letter`` existent, avec le bon nombre de
  partitions, un facteur de réplication de 1 et une rétention de 7 jours ;
- les 8 tables du PRD §6.2, plus ``transactions_staging`` et ``stream_batches``
  (phase 3), existent dans le schéma ``public`` de SENPAY_PG_DB ;
- un UPDATE de ``alerts.detected_at`` échoue (essai dans une transaction annulée).

Paramètres lus dans l'environnement (``make check`` charge ``.env``) :
SENPAY_PG_HOST, SENPAY_PG_PORT, SENPAY_PG_USER, SENPAY_PG_PASSWORD, SENPAY_PG_DB,
SENPAY_KAFKA_HOST_PORT, SENPAY_KAFKA_BOOTSTRAP_SERVERS.

Codes de sortie : 0 = tout est conforme ; 1 = au moins un écart ;
2 = configuration manquante ou service injoignable.

La comparaison (fonctions ``comparer_*``) est séparée de la lecture des
services pour être testée sans Kafka ni PostgreSQL (tests/test_check_infra.py).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass

import psycopg
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

# Délai maximal d'une requête vers le broker, en secondes
DELAI_KAFKA_S = 10.0

SEPT_JOURS_MS = 7 * 24 * 3600 * 1000  # 604 800 000 ms

# Nom du projet Docker Compose (clé `name` de docker-compose.yml) : sert à
# retrouver les conteneurs par leurs étiquettes
PROJET_COMPOSE = "mobile-money-fraud-streaming"

# Seule adresse de publication autorisée : jamais le réseau local
IP_PUBLICATION = "127.0.0.1"

# Une liaison de port publiée : (adresse IP de l'hôte, port de l'hôte)
Liaison = tuple[str, int]


@dataclass(frozen=True)
class PortAttendu:
    """Publication attendue d'un port de conteneur sur l'hôte."""

    service: str  # nom du service Compose
    port_conteneur: int  # port écouté dans le conteneur
    port_hote: int  # port attendu sur l'hôte (lu dans .env)


@dataclass(frozen=True)
class TopicAttendu:
    """Configuration attendue d'un topic (cadrage P2 phase 0, §2.4)."""

    nom: str
    partitions: int
    replication: int
    retention_ms: int


@dataclass(frozen=True)
class TopicObserve:
    """Ce que le broker renvoie pour un topic existant."""

    partitions: int
    replication: int  # plus petit nombre de répliques parmi les partitions
    retention_ms: int | None  # None si la valeur n'a pas pu être lue


TOPICS_ATTENDUS: tuple[TopicAttendu, ...] = (
    TopicAttendu("transactions", partitions=3, replication=1, retention_ms=SEPT_JOURS_MS),
    TopicAttendu("dead_letter", partitions=1, replication=1, retention_ms=SEPT_JOURS_MS),
)

TABLES_ATTENDUES: frozenset[str] = frozenset(
    {
        "transactions",
        "alerts",
        "kpi_minute",
        "user_profiles",
        "injected_anomalies",
        "generator_runs",
        "daily_kpi",
        "detection_report",
        # Phase 3 : transit du consommateur et bilan par micro-batch (ajout au PRD §6.2)
        "transactions_staging",
        "stream_batches",
    }
)

VARIABLES_REQUISES = (
    "SENPAY_PG_HOST",
    "SENPAY_PG_PORT",
    "SENPAY_PG_USER",
    "SENPAY_PG_PASSWORD",
    "SENPAY_PG_DB",
    "SENPAY_KAFKA_HOST_PORT",
    "SENPAY_KAFKA_BOOTSTRAP_SERVERS",
)


class ServiceInjoignable(Exception):
    """Kafka ou PostgreSQL ne répond pas : le contrôle n'a pas pu être fait."""


# --- Comparaison (pure, testée sans service) --------------------------------


def comparer_topics(
    attendus: tuple[TopicAttendu, ...], observes: Mapping[str, TopicObserve]
) -> list[str]:
    """Renvoie la liste des écarts entre les topics attendus et observés (vide = conforme).

    Les topics supplémentaires (dont les topics internes ``__…``) sont ignorés.
    """
    ecarts: list[str] = []
    for attendu in attendus:
        observe = observes.get(attendu.nom)
        if observe is None:
            ecarts.append(f"topic {attendu.nom} absent")
            continue
        if observe.partitions != attendu.partitions:
            ecarts.append(
                f"topic {attendu.nom} : {observe.partitions} partition(s), "
                f"{attendu.partitions} attendue(s)"
            )
        if observe.replication != attendu.replication:
            ecarts.append(
                f"topic {attendu.nom} : facteur de réplication {observe.replication}, "
                f"{attendu.replication} attendu"
            )
        if observe.retention_ms != attendu.retention_ms:
            ecarts.append(
                f"topic {attendu.nom} : retention.ms = {observe.retention_ms}, "
                f"{attendu.retention_ms} attendu"
            )
    return ecarts


def comparer_tables(attendues: Set[str], observees: Set[str]) -> list[str]:
    """Renvoie les tables attendues absentes (triées), une ligne par table."""
    return [f"table {nom} absente" for nom in sorted(attendues - observees)]


def comparer_ports(
    attendus: Sequence[PortAttendu], observes: Mapping[str, Sequence[Liaison]]
) -> list[str]:
    """Renvoie les écarts de publication des ports (vide = conforme).

    Conforme = exactement une liaison, sur 127.0.0.1 et au port attendu. Une
    liaison supplémentaire (0.0.0.0, ::) est un écart : le port serait exposé
    au réseau local.
    """
    ecarts: list[str] = []
    for attendu in attendus:
        liaisons = observes.get(attendu.service, ())
        cible = f"{IP_PUBLICATION}:{attendu.port_hote}"
        if not liaisons:
            ecarts.append(
                f"service {attendu.service} : port {attendu.port_conteneur} non publié, "
                f"{cible} attendu (conteneur arrêté ?)"
            )
        elif set(liaisons) != {(IP_PUBLICATION, attendu.port_hote)}:
            vues = ", ".join(f"{ip}:{port}" for ip, port in sorted(liaisons))
            ecarts.append(
                f"service {attendu.service} : port {attendu.port_conteneur} publié sur "
                f"{vues}, {cible} attendu"
            )
    return ecarts


# --- Lecture des services ---------------------------------------------------


def _docker(*arguments: str) -> str:
    """Lance une commande docker et renvoie sa sortie standard."""
    try:
        resultat = subprocess.run(
            ["docker", *arguments], capture_output=True, text=True, check=True, timeout=30
        )
    except (OSError, subprocess.SubprocessError) as erreur:
        raise ServiceInjoignable(f"Docker : {erreur}") from erreur
    return resultat.stdout


def lire_ports(attendus: Sequence[PortAttendu]) -> dict[str, list[Liaison]]:
    """Lit, pour chaque service, les liaisons publiées de son port de conteneur.

    Un service sans conteneur démarré est absent du résultat.
    """
    observes: dict[str, list[Liaison]] = {}
    for attendu in attendus:
        identifiant = _docker(
            "ps",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={PROJET_COMPOSE}",
            "--filter",
            f"label=com.docker.compose.service={attendu.service}",
        ).strip()
        if not identifiant:
            continue
        # Forme : {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "5434"}], ...}
        ports = json.loads(
            _docker("inspect", "--format", "{{json .NetworkSettings.Ports}}", identifiant)
        )
        liaisons = ports.get(f"{attendu.port_conteneur}/tcp") or []
        observes[attendu.service] = [(li["HostIp"], int(li["HostPort"])) for li in liaisons]
    return observes


def lire_topics(bootstrap: str, noms: list[str]) -> dict[str, TopicObserve]:
    """Lit partitions, réplication et rétention des topics demandés qui existent."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    try:
        metadonnees = admin.list_topics(timeout=DELAI_KAFKA_S)
    except KafkaException as erreur:
        raise ServiceInjoignable(f"Kafka ({bootstrap}) : {erreur}") from erreur

    presents = [nom for nom in noms if nom in metadonnees.topics]
    retentions: dict[str, int | None] = {nom: None for nom in presents}
    if presents:
        ressources = [ConfigResource(ResourceType.TOPIC, nom) for nom in presents]
        for ressource, futur in admin.describe_configs(
            ressources, request_timeout=DELAI_KAFKA_S
        ).items():
            entree = futur.result().get("retention.ms")
            if entree is not None and entree.value is not None:
                retentions[ressource.name] = int(entree.value)

    observes: dict[str, TopicObserve] = {}
    for nom in presents:
        partitions = metadonnees.topics[nom].partitions
        observes[nom] = TopicObserve(
            partitions=len(partitions),
            replication=min((len(p.replicas) for p in partitions.values()), default=0),
            retention_ms=retentions[nom],
        )
    return observes


def _connecter(env: Mapping[str, str]) -> psycopg.Connection[tuple[object, ...]]:
    """Ouvre une connexion à la base du projet (ServiceInjoignable si elle échoue)."""
    try:
        return psycopg.connect(
            host=env["SENPAY_PG_HOST"],
            port=int(env["SENPAY_PG_PORT"]),
            user=env["SENPAY_PG_USER"],
            password=env["SENPAY_PG_PASSWORD"],
            dbname=env["SENPAY_PG_DB"],
            connect_timeout=10,
        )
    except psycopg.OperationalError as erreur:
        cible = f"{env['SENPAY_PG_HOST']}:{env['SENPAY_PG_PORT']}/{env['SENPAY_PG_DB']}"
        raise ServiceInjoignable(f"PostgreSQL ({cible}) : {erreur}") from erreur


def lire_tables(env: Mapping[str, str]) -> set[str]:
    """Liste les tables ordinaires du schéma public de SENPAY_PG_DB."""
    with _connecter(env) as connexion:
        lignes = connexion.execute(
            "select table_name from information_schema.tables "
            "where table_schema = 'public' and table_type = 'BASE TABLE'"
        ).fetchall()
    return {str(ligne[0]) for ligne in lignes}


# Alerte d'essai : jamais validée, la transaction est toujours annulée
ALERTE_ESSAI_ID = "0" * 64


def verifier_detected_at_immuable(env: Mapping[str, str]) -> list[str]:
    """Vérifie que la base refuse tout UPDATE qui modifie alerts.detected_at.

    Dans une seule transaction, annulée dans tous les cas (la base n'est jamais
    modifiée) :
    1. insère une alerte R2 d'essai ;
    2. met à jour ``details`` sans toucher ``detected_at`` : doit réussir
       (c'est ce que fait l'upsert monotone de R1) ;
    3. décale ``detected_at`` d'une heure : doit échouer (trigger
       ``alerts_detected_at_immuable``, erreur P0001).

    Renvoie la liste des écarts (vide = conforme).
    """
    with _connecter(env) as connexion:
        try:
            connexion.execute(
                "insert into alerts (alert_id, rule_id, transaction_id, sender_id, severity, "
                "event_time, ingested_at) "
                "values (%s, 'R2', gen_random_uuid(), 'U-000000', 'HAUTE', now(), now())",
                (ALERTE_ESSAI_ID,),
            )
            connexion.execute(
                "update alerts set details = '{\"essai\": true}' where alert_id = %s",
                (ALERTE_ESSAI_ID,),
            )
        except psycopg.Error as erreur:
            connexion.rollback()
            return [f"alerts : essai d'immuabilité impossible : {erreur}"]
        try:
            connexion.execute(
                "update alerts set detected_at = detected_at - interval '1 hour' "
                "where alert_id = %s",
                (ALERTE_ESSAI_ID,),
            )
        except psycopg.errors.RaiseException as erreur:
            # Refus attendu, levé par le trigger
            if "immuable" in str(erreur):
                return []
            return [f"alerts.detected_at : UPDATE refusé pour une autre raison : {erreur}"]
        except psycopg.Error as erreur:
            return [f"alerts.detected_at : UPDATE refusé pour une autre raison : {erreur}"]
        finally:
            connexion.rollback()
        return ["alerts.detected_at : UPDATE accepté, la valeur n'est pas protégée"]


# --- Point d'entrée ---------------------------------------------------------


def main() -> int:
    """Lance les contrôles, affiche le résultat et renvoie le code de sortie."""
    manquantes = [nom for nom in VARIABLES_REQUISES if not os.environ.get(nom)]
    if manquantes:
        print(f"ÉCHEC : variables manquantes : {', '.join(manquantes)} (voir .env.example)")
        return 2
    env = {nom: os.environ[nom] for nom in VARIABLES_REQUISES}
    ports_attendus = (
        PortAttendu("postgres", port_conteneur=5432, port_hote=int(env["SENPAY_PG_PORT"])),
        PortAttendu("kafka", port_conteneur=9092, port_hote=int(env["SENPAY_KAFKA_HOST_PORT"])),
    )

    # Ports d'abord : un port mal publié explique souvent un service injoignable
    try:
        ports = lire_ports(ports_attendus)
    except ServiceInjoignable as erreur:
        print(f"ÉCHEC : service injoignable : {erreur}")
        return 2
    ecarts_ports = comparer_ports(ports_attendus, ports)
    for port_attendu in ports_attendus:
        vues = ", ".join(f"{ip}:{port}" for ip, port in ports.get(port_attendu.service, []))
        print(
            f"port {port_attendu.service} {port_attendu.port_conteneur} : "
            f"publié sur {vues or 'rien'}"
        )
    for ecart in ecarts_ports:
        print(f"ÉCHEC : {ecart}")

    try:
        topics = lire_topics(
            env["SENPAY_KAFKA_BOOTSTRAP_SERVERS"], [t.nom for t in TOPICS_ATTENDUS]
        )
        tables = lire_tables(env)
        ecarts_immuabilite = verifier_detected_at_immuable(env) if "alerts" in tables else []
    except ServiceInjoignable as erreur:
        print(f"ÉCHEC : service injoignable : {erreur}")
        return 2

    for attendu in TOPICS_ATTENDUS:
        observe = topics.get(attendu.nom)
        if observe is not None:
            print(
                f"topic {attendu.nom} : {observe.partitions} partition(s), "
                f"réplication {observe.replication}, retention.ms {observe.retention_ms}"
            )
    print(f"tables (schéma public) : {len(tables & TABLES_ATTENDUES)}/{len(TABLES_ATTENDUES)}")
    supplementaires = sorted(tables - TABLES_ATTENDUES)
    if supplementaires:
        print(f"info : tables non prévues : {', '.join(supplementaires)}")

    if "alerts" in tables and not ecarts_immuabilite:
        print("alerts.detected_at : UPDATE refusé par le trigger (essai annulé)")

    ecarts = (
        comparer_topics(TOPICS_ATTENDUS, topics)
        + comparer_tables(TABLES_ATTENDUES, tables)
        + ecarts_immuabilite
    )
    for ecart in ecarts:
        print(f"ÉCHEC : {ecart}")
    if ecarts or ecarts_ports:
        return 1
    print(
        f"OK : ports publiés sur 127.0.0.1, 2 topics conformes, {len(TABLES_ATTENDUES)} tables "
        "présentes, "
        "detected_at immuable"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
