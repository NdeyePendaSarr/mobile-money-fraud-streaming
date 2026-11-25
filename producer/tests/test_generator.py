"""Tests n° 2 à 5, 7 et 8 du cadrage de la phase 2 (§4), sans Docker."""

from __future__ import annotations

import hashlib
import math
import random
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from producer.anomalies import Episode, generate_episodes
from producer.config import GeneratorConfig
from producer.events import EXPECTED_RULE, OUTGOING_TYPES, Emission, to_ms
from producer.producer import Plan, plan_from_episodes, plan_run, write_dry_run
from producer.profiles import bootstrap_profiles, build_population, expected_outgoing_30d
from producer.tests.conftest import SEED, SIM_END, SIM_START
from producer.tests.schema_v1 import SchemaError, validate
from producer.traffic import make_transaction

DAKAR = ZoneInfo("Africa/Dakar")


def sha256_dry_run(config: GeneratorConfig, seed: int, tmp: Path) -> str:
    """sha256 du fichier --dry-run de 2 h simulées (nuit et matin : tous les types possibles)."""
    plan = plan_run(config, seed, SIM_START, SIM_START + timedelta(hours=2))
    out = tmp / f"run_{seed}.jsonl"
    _, digest = write_dry_run(plan, out)
    assert digest == hashlib.sha256(out.read_bytes()).hexdigest()
    return digest


# --- n° 2 : reproductibilité ------------------------------------------------


def test_meme_graine_meme_fichier(config: GeneratorConfig, tmp_path: Path) -> None:
    premier = sha256_dry_run(config, SEED, tmp_path / "a")
    second = sha256_dry_run(config, SEED, tmp_path / "b")
    assert premier == second


def test_autre_graine_autre_fichier(config: GeneratorConfig, tmp_path: Path) -> None:
    assert sha256_dry_run(config, SEED, tmp_path) != sha256_dry_run(config, SEED + 1, tmp_path)


def test_transaction_id_uuid_v4_uniques(day_emissions: list[Emission]) -> None:
    ids = [e.transaction.transaction_id for e in day_emissions]
    assert all(i.version == 4 and i.variant == uuid.RFC_4122 for i in ids)
    assert len(set(ids)) == len(ids)


# --- n° 3 : contrat du schéma v1 (partie « valides » ; invalides : H15) -----


def test_toutes_les_transactions_valides(day_emissions: list[Emission]) -> None:
    assert len(day_emissions) > 25_000  # ≈ 30 000 par jour
    for emission in day_emissions:
        message = validate(emission.value, ingested_at=SIM_END)
        assert message["sender_id"] == emission.key  # clé Kafka = sender_id


@pytest.mark.parametrize(
    ("modification", "motif"),
    [
        (lambda m: m.replace('"schema_version":1', '"schema_version":2'), "SCHEMA_VERSION"),
        (lambda m: m[:-1], "JSON_ILLISIBLE"),
        (lambda m: m.replace('"type":"', '"type":"X'), "TYPE_INCONNU"),
    ],
)
def test_le_validateur_n_accepte_pas_tout(
    day_emissions: list[Emission], modification: Callable[[str], str], motif: str
) -> None:
    # Garde-fou du test précédent : le validateur refuse bien un message abîmé
    with pytest.raises(SchemaError) as refus:
        validate(modification(day_emissions[0].value))
    assert refus.value.reason == motif


# --- n° 4 : contraintes des épisodes (§2), sur 30 jours ---------------------


@pytest.fixture(scope="module")
def plan_30j(config: GeneratorConfig) -> Plan:
    """Épisodes de 30 jours simulés (le trafic normal n'est pas généré : inutile ici)."""
    debut = datetime(2026, 10, 1, tzinfo=UTC)
    fin = debut + timedelta(days=30)
    population = build_population(config, SEED)
    episodes = generate_episodes(config, population, debut, fin, SEED)
    return Plan(config, SEED, debut, fin, population, episodes)


def test_nombre_d_episodes_par_type(config: GeneratorConfig, plan_30j: Plan) -> None:
    plan = plan_30j
    attendus = {
        "RAFALE": config.episodes.rafale.per_day,
        "MONTANT_ANORMAL": config.episodes.montant_anormal.per_day,
        "NUIT_GROS_MONTANT": config.episodes.nuit_gros_montant.per_day,
    }
    for anomaly_type, compte in plan.episodes_by_type.items():
        moyenne = 30 * attendus[anomaly_type]
        # Loi de Poisson : écart-type √moyenne ; ±5 écarts-types
        assert abs(compte - moyenne) < 5 * math.sqrt(moyenne), anomaly_type


def test_rafales(config: GeneratorConfig, plan_30j: Plan) -> None:
    plan = plan_30j
    cfg = config.episodes.rafale
    rafales = [e for e in plan.episodes if e.anomaly_type == "RAFALE"]
    assert rafales
    for episode in rafales:
        txs = episode.transactions
        assert cfg.min_transfers <= len(txs) <= cfg.max_transfers
        assert {tx.type for tx in txs} == {"TRANSFER"}
        assert len({tx.sender_id for tx in txs}) == 1
        assert {tx.channel for tx in txs} <= {"APP", "USSD"}
        duree = txs[-1].event_time - txs[0].event_time
        assert timedelta(0) <= duree <= timedelta(seconds=cfg.max_duration_s)
        destinataires = [tx.receiver_id for tx in txs]
        assert len(set(destinataires)) == len(destinataires)
        assert txs[0].sender_id not in destinataires


def test_montants_anormaux(config: GeneratorConfig, plan_30j: Plan) -> None:
    plan = plan_30j
    cfg = config.episodes.montant_anormal
    clients = {c.user_id: c for c in plan.population.clients}
    episodes = [e for e in plan.episodes if e.anomaly_type == "MONTANT_ANORMAL"]
    assert episodes
    for episode in episodes:
        (tx,) = episode.transactions
        client = clients[tx.sender_id]
        assert tx.type in OUTGOING_TYPES
        assert expected_outgoing_30d(config, client) >= cfg.min_history
        assert tx.amount_xof > 10 * client.median_amount_xof
        assert cfg.floor_xof <= tx.amount_xof <= config.amounts.max_xof
        # De jour : ne déclenche pas R3 par construction
        assert cfg.hour_start <= tx.event_time.astimezone(DAKAR).hour < cfg.hour_end


def test_nuits_gros_montant(config: GeneratorConfig, plan_30j: Plan) -> None:
    plan = plan_30j
    cfg = config.episodes.nuit_gros_montant
    episodes = [e for e in plan.episodes if e.anomaly_type == "NUIT_GROS_MONTANT"]
    assert episodes
    for episode in episodes:
        (tx,) = episode.transactions
        assert tx.type in OUTGOING_TYPES
        # Bornes de la configuration, et seuil de R3 (> 500 000) franchi
        assert cfg.amount_min_xof <= tx.amount_xof <= cfg.amount_max_xof
        assert 500_000 < tx.amount_xof <= 2_000_000
        assert cfg.hour_start <= tx.event_time.astimezone(DAKAR).hour < cfg.hour_end
        assert 1 <= tx.event_time.astimezone(DAKAR).hour < 5


# --- n° 5 : vérité terrain --------------------------------------------------


def test_verite_terrain(day_plan: Plan, day_emissions: list[Emission]) -> None:
    attendues = {
        tx.transaction_id: episode for episode in day_plan.episodes for tx in episode.transactions
    }
    vues: Counter[uuid.UUID] = Counter()
    for emission in day_emissions:
        tx_id = emission.transaction.transaction_id
        if emission.truth is None:
            # Une transaction normale n'appartient à aucun épisode
            assert tx_id not in attendues
            continue
        vues[tx_id] += 1
        episode = attendues[tx_id]
        assert emission.truth.anomaly_id == episode.anomaly_id
        assert emission.truth.anomaly_type == episode.anomaly_type
        assert emission.truth.expected_rule == EXPECTED_RULE[episode.anomaly_type]
        # Jamais invalide, jamais très en retard (H12) : noyau = aucun retard
        validate(emission.value)
        assert emission.emit_sim_time == emission.transaction.event_time
    # Chaque transaction d'épisode est envoyée exactement une fois
    assert set(vues) == set(attendues)
    assert set(vues.values()) == {1}


def test_verite_absente_du_message(day_emissions: list[Emission]) -> None:
    # F1 : aucun champ de vérité terrain dans ce qui part dans Kafka
    for emission in day_emissions:
        if emission.truth is not None:
            assert "anomaly" not in emission.value and "RAFALE" not in emission.value


def test_couples_type_regle(day_plan: Plan) -> None:
    for episode in day_plan.episodes:
        assert (episode.anomaly_type, episode.expected_rule) in EXPECTED_RULE.items()


def test_rafale_a_cheval_sur_deux_heures(config: GeneratorConfig, day_plan: Plan) -> None:
    # Rafale de 8 transferts de 10:59:30 à 11:00:40 (70 s) : elle déborde sur la
    # tranche de 11 h. Chaque transaction doit partir une fois, dans la tranche de
    # son event_time, avec sa vérité terrain, et l'ordre global rester trié.
    debut = SIM_START + timedelta(hours=10)
    fin = debut + timedelta(hours=2)
    rng = random.Random("test:rafale")
    population = day_plan.population
    client = population.clients[0]
    t0 = SIM_START + timedelta(hours=10, minutes=59, seconds=30)
    offsets_s = [0, 5, 12, 20, 29, 31, 50, 70]  # 5 avant 11:00:00, 3 après
    txs = tuple(
        make_transaction(
            rng,
            config,
            population,
            client,
            "TRANSFER",
            to_ms(t0 + timedelta(seconds=s)),
            1_000,
            client.home_region,
            channel="APP",
            receiver_id=population.clients[i + 1].user_id,
        )
        for i, s in enumerate(offsets_s)
    )
    rafale = Episode(uuid.UUID(int=1, version=4), "RAFALE", txs)
    plan = plan_from_episodes(config, SEED, debut, fin, population, [rafale])

    tranches = list(plan.slices())
    assert len(tranches) == 2  # 10 h et 11 h
    onze_heures = debut + timedelta(hours=1)
    par_tranche = [[e for e in tranche if e.truth is not None] for tranche in tranches]
    assert [len(t) for t in par_tranche] == [5, 3]
    assert all(e.transaction.event_time < onze_heures for e in par_tranche[0])
    assert all(e.transaction.event_time >= onze_heures for e in par_tranche[1])
    envoyees = [e.transaction.transaction_id for t in par_tranche for e in t]
    assert envoyees == [tx.transaction_id for tx in txs]  # une fois chacune, dans l'ordre
    assert all(e.truth == par_tranche[0][0].truth for t in par_tranche for e in t)
    instants = [e.emit_sim_time for tranche in tranches for e in tranche]
    assert instants == sorted(instants)


# --- n° 7 : ordre -------------------------------------------------------------


def test_emissions_triees_et_dans_la_plage(day_emissions: list[Emission]) -> None:
    instants = [e.emit_sim_time for e in day_emissions]
    assert instants == sorted(instants)
    for emission in day_emissions:
        assert emission.transaction.event_time <= emission.emit_sim_time
        assert SIM_START <= emission.transaction.event_time < SIM_END


def test_ordre_par_client_conserve(day_emissions: list[Emission]) -> None:
    # Même clé → même partition : l'ordre d'envoi par client suit event_time
    par_client: defaultdict[str, list[datetime]] = defaultdict(list)
    for emission in day_emissions:
        par_client[emission.key].append(emission.transaction.event_time)
    assert all(t == sorted(t) for t in par_client.values())


# --- n° 8 : profils d'amorçage ----------------------------------------------


def test_profils(config: GeneratorConfig, day_plan: Plan) -> None:
    profils = bootstrap_profiles(config, day_plan.population, SIM_START)
    assert len(profils) == config.population.n_clients == 10_000
    assert len({p.user_id for p in profils}) == 10_000
    clients = {c.user_id: c for c in day_plan.population.clients}
    for profil in profils:
        client = clients[profil.user_id]
        assert profil.median_amount_xof == round(math.exp(client.mu))
        assert profil.home_region == client.home_region
        anciennete = SIM_START - profil.first_seen
        assert timedelta(days=30) <= anciennete <= timedelta(days=720)


def test_mediane_d_amorcage_coherente_avec_le_trafic(
    config: GeneratorConfig, day_plan: Plan, day_emissions: list[Emission]
) -> None:
    # Cohérence H9 : chaque sortie normale, centrée sur le mu de son client et
    # réduite par sigma, suit une loi normale centrée ; sa médiane globale est ≈ 0.
    # Si la médiane d'amorçage exp(mu) ne correspondait pas au trafic, elle s'écarterait.
    clients = {c.user_id: c for c in day_plan.population.clients}
    sigma = config.population.amount_sigma
    z = sorted(
        (math.log(e.transaction.amount_xof) - clients[e.key].mu) / sigma
        for e in day_emissions
        if e.truth is None and e.transaction.type in OUTGOING_TYPES
    )
    assert len(z) > 20_000
    assert abs(z[len(z) // 2]) < 0.05
