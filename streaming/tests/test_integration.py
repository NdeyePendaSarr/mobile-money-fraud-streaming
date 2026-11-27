"""Tests d'intégration de la phase 4 (``make test-integration``, services démarrés).

- ``test_rejeu_de_la_fusion_sans_doublon`` : la transaction de fusion (transactions,
  alertes, transits, bilan) rejouée deux fois sur la vraie base n'écrit qu'une fois ;
  le tout tourne dans une transaction annulée à la fin : la base n'est pas modifiée.
- ``test_j12_profil_modifie_vu_au_lot_suivant`` (J12, R2-10 de la phase 0) : **exige
  le job allumé** (``make stream``). Témoin positif (médiane 10 000 → alerte R2), puis
  médiane passée à 100 000 en base → même montant, plus d'alerte. Ses messages ne
  sont pas dans ``generator_runs`` : il passe en dernier, puis ``make reset``.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from confluent_kafka import Producer

from producer.events import Transaction
from streaming.sinks import BatchSummary, merge_batch

pytestmark = pytest.mark.integration

TOPIC = "transactions"
# Client réservé, hors de la population du générateur (U-000001 à U-010000)
CLIENT_J12 = "U-999999"
# Au plus deux déclenchements de 5 s, plus la durée d'un lot
DELAI_MAX_S = 15.0


def _connect() -> psycopg.Connection[tuple[Any, ...]]:
    return psycopg.connect(
        host=os.environ["SENPAY_PG_HOST"],
        port=int(os.environ["SENPAY_PG_PORT"]),
        user=os.environ["SENPAY_PG_USER"],
        password=os.environ["SENPAY_PG_PASSWORD"],
        dbname=os.environ["SENPAY_PG_DB"],
    )


# --- Rejeu de la fusion --------------------------------------------------------


def test_rejeu_de_la_fusion_sans_doublon() -> None:
    lot = 10**12  # numéro de lot hors de portée du job
    tx_id = str(uuid.uuid4())
    alert_id = "a" * 64
    maintenant = datetime.now(UTC)
    with _connect() as db, db.transaction(force_rollback=True):
        db.execute(
            "insert into transactions_staging (batch_id, transaction_id, schema_version, "
            "event_time, sender_id, receiver_id, type, amount_xof, region, channel, agent_id, "
            "ingested_at, kafka_partition, kafka_offset) values "
            "(%s, %s, 1, %s, 'U-999998', 'U-000001', 'TRANSFER', 600000, 'Dakar', 'APP', null, "
            "%s, 0, %s)",
            (lot, tx_id, maintenant, maintenant, 10**12),
        )
        db.execute(
            "insert into alerts_staging (batch_id, alert_id, rule_id, transaction_id, sender_id, "
            "severity, event_time, ingested_at, details) values "
            "(%s, %s, 'R3', %s, 'U-999998', 'MOYENNE', %s, %s, '{\"montant_xof\": 600000}')",
            (lot, alert_id, tx_id, maintenant, maintenant),
        )
        bilan = BatchSummary(lot, 1, 1, 0, {}, {"R3": 1})

        premier = merge_batch(db, lot, bilan, maintenant)
        detected_at = db.execute(
            "select detected_at from alerts where alert_id = %s", (alert_id,)
        ).fetchone()
        ligne_bilan = db.execute(
            "select nb_inseres, nb_alerts_by_rule, processed_at from stream_batches "
            "where batch_id = %s",
            (lot,),
        ).fetchone()
        # Le rejeu reçoit de nouveau les mêmes lignes en transit (même lot, mêmes offsets)
        db.execute(
            "insert into alerts_staging (batch_id, alert_id, rule_id, transaction_id, sender_id, "
            "severity, event_time, ingested_at, details) values "
            "(%s, %s, 'R3', %s, 'U-999998', 'MOYENNE', %s, %s, '{\"montant_xof\": 600000}')",
            (lot, alert_id, tx_id, maintenant, maintenant),
        )
        second = merge_batch(db, lot, bilan, maintenant)

        assert (premier.nb_inseres, premier.nb_alertes_inserees, premier.bilan_ecrit) == (
            1,
            1,
            True,
        )
        assert (second.nb_inseres, second.nb_alertes_inserees, second.bilan_ecrit) == (0, 0, False)
        nb = db.execute("select count(*) from alerts where alert_id = %s", (alert_id,)).fetchone()
        assert nb is not None and nb[0] == 1
        apres = db.execute(
            "select detected_at from alerts where alert_id = %s", (alert_id,)
        ).fetchone()
        assert apres == detected_at  # detected_at inchangé
        bilan_apres = db.execute(
            "select nb_inseres, nb_alerts_by_rule, processed_at from stream_batches "
            "where batch_id = %s",
            (lot,),
        ).fetchone()
        assert bilan_apres == ligne_bilan  # bilan d'origine intact
        reste = db.execute(
            "select (select count(*) from transactions_staging where batch_id = %s) + "
            "(select count(*) from alerts_staging where batch_id = %s)",
            (lot, lot),
        ).fetchone()
        assert reste is not None and reste[0] == 0


# --- J12 : profil modifié pris en compte au lot suivant --------------------------


def _envoyer(producteur: Producer, montant: int) -> str:
    """Envoie un TRANSFER du client réservé, à l'heure réelle ; renvoie son transaction_id."""
    tx = Transaction(
        transaction_id=uuid.uuid4(),
        event_time=datetime.now(UTC),
        sender_id=CLIENT_J12,
        receiver_id="U-000001",
        type="TRANSFER",
        amount_xof=montant,
        region="Dakar",
        channel="APP",
        agent_id=None,
    )
    producteur.produce(TOPIC, key=tx.sender_id.encode(), value=tx.to_json().encode())
    assert producteur.flush(10) == 0
    return str(tx.transaction_id)


def _attendre_transaction(tx_id: str) -> float:
    """Attend la transaction en base ; renvoie le délai (s). Échec si le job ne suit pas."""
    debut = time.monotonic()
    while time.monotonic() - debut < DELAI_MAX_S:
        with _connect() as db:
            row = db.execute(
                "select 1 from transactions where transaction_id = %s", (tx_id,)
            ).fetchone()
        if row is not None:
            return time.monotonic() - debut
        time.sleep(0.5)
    pytest.fail(f"transaction {tx_id} absente après {DELAI_MAX_S} s : job arrêté ? (make stream)")


def _alertes_r2(tx_id: str) -> int:
    with _connect() as db:
        row = db.execute(
            "select count(*) from alerts where transaction_id = %s and rule_id = 'R2'", (tx_id,)
        ).fetchone()
    assert row is not None
    return int(row[0])


def test_j12_profil_modifie_vu_au_lot_suivant() -> None:
    with _connect() as db:
        db.execute(
            "insert into user_profiles (user_id, median_amount_xof, nb_tx_30j, home_region, "
            "first_seen, source) values (%s, 10000, 20, 'Dakar', %s, 'GENERATEUR') "
            "on conflict (user_id) do update set median_amount_xof = 10000, nb_tx_30j = 20",
            (CLIENT_J12, datetime.now(UTC) - timedelta(days=60)),
        )
    producteur = Producer(
        {"bootstrap.servers": os.environ["SENPAY_KAFKA_BOOTSTRAP_SERVERS"], "acks": "all"}
    )

    # (1) Témoin positif : 200 000 > 10 × 10 000, ≥ 50 000, 20 transactions → alerte R2.
    # 200 000 < 500 000 : R3 ne peut pas interférer.
    temoin = _envoyer(producteur, 200_000)
    delai_1 = _attendre_transaction(temoin)
    assert _alertes_r2(temoin) == 1, "témoin positif sans alerte R2 : la règle ne tourne pas"

    # (2) Médiane passée à 100 000 en base, sans toucher au job
    with _connect() as db:
        db.execute(
            "update user_profiles set median_amount_xof = 100000 where user_id = %s", (CLIENT_J12,)
        )

    # (3) Même montant : 200 000 ≤ 10 × 100 000 → pas d'alerte, transaction bien en base
    # (alertes et transactions sont validées dans la même transaction : l'absence est définitive)
    second = _envoyer(producteur, 200_000)
    delai_2 = _attendre_transaction(second)
    assert _alertes_r2(second) == 0, "alerte R2 malgré la médiane modifiée : profil en cache ?"
    print(f"J12 : délais de prise en compte {delai_1:.1f} s puis {delai_2:.1f} s")
