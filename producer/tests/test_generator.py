"""Tests n° 2 à 5, 7 et 8 du cadrage de la phase 2 (§4), sans Docker."""

from __future__ import annotations

import hashlib
import math
import random
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from producer.anomalies import CORE_TYPES, Episode, generate_episodes
from producer.config import GeneratorConfig
from producer.delivery import INVALID_REASONS, corrupt
from producer.events import EXPECTED_RULE, OUTGOING_TYPES, Emission, to_ms
from producer.producer import (
    Plan,
    RunError,
    check_seed_unused,
    plan_from_episodes,
    plan_run,
    write_dry_run,
)
from producer.profiles import bootstrap_profiles, build_population, expected_outgoing_30d
from producer.tests.conftest import SEED, SIM_END, SIM_START
from producer.tests.schema_v1 import SchemaError, validate
from producer.traffic import generate_near_threshold, make_transaction

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


# --- n° 3 : contrat du schéma v1 --------------------------------------------

# Heure réelle d'envoi fictive pour les tests (≥ fin de la plage simulée)
REEL = SIM_END + timedelta(days=1)


def test_toutes_les_transactions_valides(day_emissions: list[Emission]) -> None:
    assert len(day_emissions) > 25_000  # ≈ 30 000 par jour
    valides = [e for e in day_emissions if e.invalid_reason is None]
    assert len(valides) > 0.99 * len(day_emissions)
    for emission in valides:
        message = validate(emission.render(REEL), ingested_at=REEL)
        assert message["sender_id"] == emission.key  # clé Kafka = sender_id


def test_chaque_invalide_refuse_pour_son_motif(day_emissions: list[Emission]) -> None:
    invalides = [e for e in day_emissions if e.invalid_reason is not None]
    assert {e.invalid_reason for e in invalides} == set(INVALID_REASONS)  # 8 motifs vus
    for emission in invalides:
        with pytest.raises(SchemaError) as refus:
            validate(emission.render(REEL), ingested_at=REEL)
        assert refus.value.reason == emission.invalid_reason
        assert emission.key == emission.transaction.sender_id  # clé inchangée


@pytest.mark.parametrize("motif", INVALID_REASONS)
def test_corruption_systematique(day_emissions: list[Emission], motif: str) -> None:
    # Chaque motif appliqué à 300 transactions de tous types : refus pour ce motif-là
    rng = random.Random(f"test:{motif}")
    for emission in day_emissions[:300]:
        tx = emission.transaction
        valeur = corrupt(rng, tx, motif)
        if valeur is None:  # EVENT_TIME_FUTUR : construit à l'envoi
            valeur = Emission(
                tx.event_time, tx.sender_id, None, tx, None, None, motif, 6 * 60_000
            ).render(REEL)
        with pytest.raises(SchemaError) as refus:
            validate(valeur, ingested_at=REEL)
        assert refus.value.reason == motif


def test_motif_futur_calcule_a_l_envoi(day_emissions: list[Emission]) -> None:
    futurs = [e for e in day_emissions if e.invalid_reason == "EVENT_TIME_FUTUR"]
    assert futurs
    emission = futurs[0]
    assert emission.value is None and emission.future_offset_ms is not None
    # Deux heures d'envoi différentes → deux messages différents, toujours refusés
    premier, second = emission.render(REEL), emission.render(REEL + timedelta(hours=1))
    assert premier != second
    for valeur, reel in ((premier, REEL), (second, REEL + timedelta(hours=1))):
        with pytest.raises(SchemaError):
            validate(valeur, ingested_at=reel)


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
        validate(modification(day_emissions[0].transaction.to_json()))
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
        "CASH_OUT_RAPIDE": config.episodes.cash_out_rapide.per_day,
        "CHANGEMENT_REGION": config.episodes.changement_region.per_day,
    }
    assert set(plan.episodes_by_type) == set(attendus)
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


def test_cash_outs_rapides(config: GeneratorConfig, plan_30j: Plan) -> None:
    plan = plan_30j
    agents_region = {a: r for r, agents in plan.population.agents_by_region.items() for a in agents}
    episodes = [e for e in plan.episodes if e.anomaly_type == "CASH_OUT_RAPIDE"]
    assert episodes
    for episode in episodes:
        cash_in, cash_out = episode.transactions
        assert (cash_in.type, cash_out.type) == ("CASH_IN", "CASH_OUT")
        assert cash_in.sender_id == cash_out.sender_id
        # Délai entre 1 et 9 min, toujours < 10 min
        delai = cash_out.event_time - cash_in.event_time
        assert timedelta(minutes=1) <= delai <= timedelta(minutes=9) < timedelta(minutes=10)
        # Au moins 90 % du dépôt, jamais plus
        assert 0.90 * cash_in.amount_xof <= cash_out.amount_xof <= cash_in.amount_xof
        # Agents de la même région, qui est celle des deux opérations
        assert cash_in.agent_id is not None and cash_out.agent_id is not None
        assert agents_region[cash_in.agent_id] == agents_region[cash_out.agent_id]
        assert cash_in.region == cash_out.region == agents_region[cash_in.agent_id]


def test_changements_de_region(config: GeneratorConfig, plan_30j: Plan) -> None:
    plan = plan_30j
    paires = {frozenset(p) for p in config.episodes.changement_region.far_pairs}
    assert paires == {
        frozenset(("Ziguinchor", r))
        for r in ("Dakar", "Thiès", "Diourbel", "Saint-Louis", "Kaolack")
    } | {frozenset(("Saint-Louis", "Kaolack"))}
    episodes = [e for e in plan.episodes if e.anomaly_type == "CHANGEMENT_REGION"]
    assert episodes
    for episode in episodes:
        premiere, seconde = episode.transactions
        assert premiere.sender_id == seconde.sender_id
        assert {premiere.type, seconde.type} <= set(OUTGOING_TYPES)
        assert frozenset((premiere.region, seconde.region)) in paires
        assert timedelta(0) < seconde.event_time - premiere.event_time < timedelta(minutes=15)


# Empreinte des épisodes R1 à R3 (graine 42, 30 jours dès le 01/10/2026), calculée
# avec le code du noyau (commit 49f8b33), avant l'ajout de R4, R5, des retards et
# des invalides : ces compléments ne doivent changer aucun épisode existant (H19).
EMPREINTE_R1_R3 = "a1b3142d168841763d8dcb154998a9a34fc8b82eae32bf3559af0d6b2a5e41f4"


def test_episodes_r1_r3_inchanges(plan_30j: Plan) -> None:
    empreinte = hashlib.sha256()
    nombre = 0
    for episode in plan_30j.episodes:
        if episode.anomaly_type in CORE_TYPES:
            ids = ",".join(str(tx.transaction_id) for tx in episode.transactions)
            empreinte.update(f"{episode.anomaly_type}|{episode.anomaly_id}|{ids}\n".encode())
            nombre += 1
    assert nombre == 2_568
    assert empreinte.hexdigest() == EMPREINTE_R1_R3


# --- Trafic près des seuils (H10) -------------------------------------------


def test_trafic_pres_des_seuils(config: GeneratorConfig, day_plan: Plan) -> None:
    proches = generate_near_threshold(config, day_plan.population, SIM_START, SIM_END, SEED)
    cfg = config.near_threshold
    attendu = (
        cfg.presque_rafale_per_day * cfg.presque_rafale_transfers
        + cfg.gros_montant_jour_per_day
        + cfg.nuit_sous_seuil_per_day
        + cfg.multiple_mediane_per_day
    )
    assert 0.5 * attendu < len(proches) < 1.5 * attendu
    # Absent de la vérité terrain, et conforme au schéma
    episodes = {tx.transaction_id for e in day_plan.episodes for tx in e.transactions}
    assert not {tx.transaction_id for tx in proches} & episodes
    for tx in proches:
        validate(tx.to_json())
    # Des montants de nuit juste sous le seuil de R3, et des gros montants de jour
    heure = {tx.transaction_id: tx.event_time.astimezone(DAKAR).hour for tx in proches}
    assert any(
        400_000 <= tx.amount_xof <= 500_000 and 1 <= heure[tx.transaction_id] < 5 for tx in proches
    )
    assert any(tx.amount_xof > 500_000 and heure[tx.transaction_id] >= 6 for tx in proches)


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
        # Jamais invalide, jamais très en retard (H12) ; retard ordinaire possible
        assert emission.invalid_reason is None
        assert emission.delay in (None, "LATE")
        validate(emission.render(REEL))
    # Chaque transaction d'épisode est envoyée exactement une fois
    assert set(vues) == set(attendues)
    assert set(vues.values()) == {1}


def test_verite_absente_du_message(day_emissions: list[Emission]) -> None:
    # F1 : aucun champ de vérité terrain dans ce qui part dans Kafka
    for emission in day_emissions:
        if emission.truth is not None:
            valeur = emission.render(REEL)
            assert "anomaly" not in valeur and "RAFALE" not in valeur


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
    # Sans retards : ce test ne regarde que le découpage par tranche
    sans_retard = replace(config, delays=replace(config.delays, late_share=0, very_late_share=0))
    plan = plan_from_episodes(sans_retard, SEED, debut, fin, population, [rafale])

    tranches = list(plan.slices())
    assert tranches[-1] == []  # aucun retardataire après la plage
    tranches = tranches[:-1]
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


# --- Graine unique par base -------------------------------------------------


def test_meme_graine_memes_identifiants_sur_une_autre_plage(config: GeneratorConfig) -> None:
    # Raison du refus : les transaction_id ne dépendent que de la graine (flux « trafic »
    # depuis son début), pas de la plage. Deux runs de graine 42 se recouvrent.
    jour_1 = plan_run(config, SEED, SIM_START, SIM_START + timedelta(hours=1))
    jour_2 = plan_run(config, SEED, SIM_END, SIM_END + timedelta(hours=1))
    premiers = [
        {e.transaction.transaction_id for e in next(iter(p.slices()))} for p in (jour_1, jour_2)
    ]
    assert premiers[0] & premiers[1]


def test_graine_deja_utilisee_refusee() -> None:
    with pytest.raises(RunError, match="make reset"):
        check_seed_unused(42, existing_run_id=1)


def test_graine_libre_acceptee() -> None:
    check_seed_unused(43, existing_run_id=None)


# --- n° 7 : ordre -------------------------------------------------------------


def test_emissions_triees_et_dans_la_plage(day_emissions: list[Emission]) -> None:
    instants = [e.emit_sim_time for e in day_emissions]
    assert instants == sorted(instants)
    for emission in day_emissions:
        assert emission.transaction.event_time <= emission.emit_sim_time
        assert SIM_START <= emission.transaction.event_time < SIM_END


def test_ordre_par_client_hors_retards(day_emissions: list[Emission]) -> None:
    # Même clé → même partition : sans retard, l'ordre d'envoi par client suit event_time.
    # Seuls les messages en retard (H14) arrivent après des messages plus récents.
    par_client: defaultdict[str, list[datetime]] = defaultdict(list)
    for emission in day_emissions:
        if emission.delay is None:
            par_client[emission.key].append(emission.transaction.event_time)
    assert all(t == sorted(t) for t in par_client.values())


# --- n° 6 : taux de retards et d'invalides (1 jour, graine 42) --------------


def test_taux(config: GeneratorConfig, day_plan: Plan, day_emissions: list[Emission]) -> None:
    n = len(day_emissions)
    retards = Counter(e.delay for e in day_emissions)
    invalides = Counter(e.invalid_reason for e in day_emissions if e.invalid_reason)
    # Les compteurs du run sont ceux des émissions
    compteurs = day_plan.counters
    assert compteurs.nb_late == retards["LATE"]
    assert compteurs.nb_very_late == retards["VERY_LATE"]
    assert compteurs.nb_invalid == sum(invalides.values())
    assert {k: v for k, v in compteurs.nb_invalid_by_reason.items() if v} == dict(invalides)
    # Tolérances ≈ 5 écarts-types d'une loi binomiale sur ≈ 30 000 messages
    assert abs(retards["LATE"] / n - config.delays.late_share) < 0.007
    assert abs(retards["VERY_LATE"] / n - config.delays.very_late_share) < 0.0013
    assert abs(sum(invalides.values()) / n - config.invalid.share) < 0.002


def test_bornes_des_retards(config: GeneratorConfig, day_emissions: list[Emission]) -> None:
    d = config.delays
    for emission in day_emissions:
        retard = emission.emit_sim_time - emission.transaction.event_time
        if emission.delay is None:
            assert retard == timedelta(0)
        elif emission.delay == "LATE":
            assert timedelta(seconds=d.late_min_s) <= retard <= timedelta(seconds=d.late_max_s)
        else:
            assert emission.truth is None  # jamais sur un épisode (H12)
            bornes = (timedelta(seconds=d.very_late_min_s), timedelta(seconds=d.very_late_max_s))
            assert bornes[0] <= retard <= bornes[1]


def test_invalides_jamais_sur_un_episode(day_emissions: list[Emission]) -> None:
    assert all(e.truth is None for e in day_emissions if e.invalid_reason is not None)


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
