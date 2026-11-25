"""Validateur du schéma de l'événement v1, **pour les tests** (cadrage phase 0, §2.1, §2.2).

Écrit indépendamment de ``producer.events`` : il vérifie le message tel qu'il part
dans Kafka, comme le fera le job Spark (phase 3). Chaque refus porte un motif, qui
correspond aux motifs d'invalidité de H15 (complément de fin de phase 2).
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta

REGIONS = {"Dakar", "Thiès", "Diourbel", "Saint-Louis", "Kaolack", "Ziguinchor"}
CHAMPS = (
    "schema_version",
    "transaction_id",
    "event_time",
    "sender_id",
    "receiver_id",
    "type",
    "amount_xof",
    "region",
    "channel",
    "agent_id",
)
EVENT_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
CLIENT = re.compile(r"^U-\d{6}$")
MARCHAND = re.compile(r"^M-\d{4}$")
AGENT = re.compile(r"^A-\d{4}$")


class SchemaError(ValueError):
    """Message refusé ; ``reason`` = motif (ex. ``CANAL_INCOHERENT``)."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason} {detail}".strip())
        self.reason = reason


def validate(raw: str, ingested_at: datetime | None = None) -> dict[str, object]:
    """Valide un message v1 et le renvoie décodé ; lève ``SchemaError`` sinon.

    ``ingested_at`` (aware) active la règle « event_time ≤ ingested_at + 5 min ».
    """
    try:
        message = json.loads(raw)
    except json.JSONDecodeError as erreur:
        raise SchemaError("JSON_ILLISIBLE", str(erreur)) from erreur
    if not isinstance(message, dict):
        raise SchemaError("JSON_ILLISIBLE", "objet attendu")
    manquants = [c for c in CHAMPS if c not in message]
    if manquants:
        raise SchemaError("CHAMP_MANQUANT", ", ".join(manquants))
    if set(message) != set(CHAMPS):
        raise SchemaError("CHAMP_INCONNU", ", ".join(sorted(set(message) - set(CHAMPS))))
    if message["schema_version"] != 1:
        raise SchemaError("SCHEMA_VERSION", repr(message["schema_version"]))

    try:
        tx_id = uuid.UUID(str(message["transaction_id"]))
    except ValueError as erreur:
        raise SchemaError("FORMAT", "transaction_id") from erreur
    if tx_id.version != 4 or str(tx_id) != message["transaction_id"]:
        raise SchemaError("FORMAT", "transaction_id : UUID v4 attendu")

    event_time = message["event_time"]
    if not isinstance(event_time, str) or not EVENT_TIME.match(event_time):
        raise SchemaError("FORMAT", "event_time")
    if ingested_at is not None:
        instant = datetime.fromisoformat(event_time)
        if instant > ingested_at + timedelta(minutes=5):
            raise SchemaError("EVENT_TIME_FUTUR", event_time)

    sender = message["sender_id"]
    if not isinstance(sender, str) or not CLIENT.match(sender):
        raise SchemaError("FORMAT", "sender_id")

    tx_type = message["type"]
    if tx_type not in ("TRANSFER", "PAYMENT", "CASH_IN", "CASH_OUT"):
        raise SchemaError("TYPE_INCONNU", repr(tx_type))

    amount = message["amount_xof"]
    if isinstance(amount, bool) or not isinstance(amount, int):
        raise SchemaError("FORMAT", "amount_xof : entier attendu")
    if not 100 <= amount <= 2_000_000:
        raise SchemaError("MONTANT_HORS_BORNES", str(amount))

    if message["region"] not in REGIONS:
        raise SchemaError("FORMAT", "region")

    channel, receiver, agent = message["channel"], message["receiver_id"], message["agent_id"]
    if tx_type in ("TRANSFER", "PAYMENT"):
        if channel not in ("APP", "USSD") or agent is not None:
            raise SchemaError("CANAL_INCOHERENT", f"{tx_type} / {channel}")
        motif = CLIENT if tx_type == "TRANSFER" else MARCHAND
        if not isinstance(receiver, str) or not motif.match(receiver):
            raise SchemaError("FORMAT", "receiver_id")
        if receiver == sender:
            raise SchemaError("RECEVEUR_EGAL_EMETTEUR", sender)
    else:
        if channel != "AGENT":
            raise SchemaError("CANAL_INCOHERENT", f"{tx_type} / {channel}")
        if not isinstance(agent, str) or not AGENT.match(agent):
            raise SchemaError("FORMAT", "agent_id")
        if receiver is not None:
            raise SchemaError("FORMAT", "receiver_id : null attendu")
    return message
