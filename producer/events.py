"""Événement « transaction » v1 : modèle, sérialisation JSON et émissions.

Le schéma est celui du cadrage de la phase 0 (§2.1 et §2.2). La vérité terrain
(type d'anomalie, règle attendue) n'est **jamais** dans le message (F1) : elle
voyage à côté, dans ``Emission.truth``, et part dans ``injected_anomalies``.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

SCHEMA_VERSION = 1

REGIONS: tuple[str, ...] = ("Dakar", "Thiès", "Diourbel", "Saint-Louis", "Kaolack", "Ziguinchor")
TYPES: tuple[str, ...] = ("TRANSFER", "PAYMENT", "CASH_IN", "CASH_OUT")
# Montants sortants pour le client (R1 à R3 ne regardent que ceux-là)
OUTGOING_TYPES: tuple[str, ...] = ("TRANSFER", "PAYMENT", "CASH_OUT")
# Canaux autorisés par type (matrice §2.2)
CHANNELS_BY_TYPE: dict[str, tuple[str, ...]] = {
    "TRANSFER": ("APP", "USSD"),
    "PAYMENT": ("APP", "USSD"),
    "CASH_IN": ("AGENT",),
    "CASH_OUT": ("AGENT",),
}

# Types d'épisodes et règle attendue (cadrage phase 0, §4)
EXPECTED_RULE: dict[str, str] = {
    "RAFALE": "R1",
    "MONTANT_ANORMAL": "R2",
    "NUIT_GROS_MONTANT": "R3",
    "CASH_OUT_RAPIDE": "R4",
    "CHANGEMENT_REGION": "R5",
}

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def uuid_from(rng: random.Random) -> uuid.UUID:
    """UUID v4 tiré d'un générateur à graine (H4).

    ``uuid.uuid4()`` lit ``os.urandom`` : il rendrait un run non reproductible
    sans aucune erreur visible.
    """
    return uuid.UUID(int=rng.getrandbits(128), version=4)


def from_ms(epoch_ms: int) -> datetime:
    """Instant UTC (aware) à partir d'un nombre de millisecondes depuis 1970."""
    return _EPOCH + timedelta(milliseconds=epoch_ms)


def to_ms(instant: datetime) -> int:
    """Millisecondes depuis 1970 d'un instant aware (tronqué à la milliseconde)."""
    if instant.tzinfo is None:
        raise ValueError("horodatage sans fuseau refusé (F9)")
    return (instant - _EPOCH) // timedelta(milliseconds=1)


def format_event_time(instant: datetime) -> str:
    """Format v1 : ``AAAA-MM-JJTHH:MM:SS.mmmZ`` (UTC, millisecondes, H16)."""
    utc = instant.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class Transaction:
    """Une transaction valide du schéma v1 (sans aucun champ de vérité terrain)."""

    transaction_id: uuid.UUID
    event_time: datetime  # temps simulé, UTC, résolution milliseconde
    sender_id: str
    receiver_id: str | None
    type: str
    amount_xof: int
    region: str
    channel: str
    agent_id: str | None

    def to_message(self) -> dict[str, object]:
        """Champs du message, dans l'ordre du schéma (§2.1)."""
        return {
            "schema_version": SCHEMA_VERSION,
            "transaction_id": str(self.transaction_id),
            "event_time": format_event_time(self.event_time),
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "type": self.type,
            "amount_xof": self.amount_xof,
            "region": self.region,
            "channel": self.channel,
            "agent_id": self.agent_id,
        }

    def to_json(self) -> str:
        """JSON compact UTF-8 (« Thiès » n'est pas échappé), ordre du schéma."""
        return json.dumps(self.to_message(), ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True)
class Truth:
    """Vérité terrain d'une transaction d'épisode : jamais envoyée dans Kafka."""

    anomaly_id: uuid.UUID
    anomaly_type: str
    expected_rule: str


@dataclass(frozen=True)
class Emission:
    """Un message à envoyer, au temps simulé ``emit_sim_time`` (H1).

    - ``emit_sim_time`` ≥ ``event_time`` : un retard (H14) décale l'émission,
      jamais ``event_time`` ; ``delay`` vaut ``None``, ``"LATE"`` ou ``"VERY_LATE"``.
    - ``transaction`` est toujours la transaction valide d'origine ; si le message
      est invalide (H15), ``invalid_reason`` donne le motif et ``value`` la
      version corrompue qui part dans Kafka (la version valide n'est pas envoyée).
    - Motif ``EVENT_TIME_FUTUR`` : la valeur dépend de l'heure réelle d'envoi.
      ``value`` vaut alors ``None`` et ``future_offset_ms`` donne le décalage ;
      ``render(heure_reelle)`` construit le message au moment de l'envoi.
    """

    emit_sim_time: datetime
    key: str  # clé Kafka = sender_id
    value: str | None  # message JSON ; None seulement pour EVENT_TIME_FUTUR
    transaction: Transaction
    truth: Truth | None
    delay: str | None = None
    invalid_reason: str | None = None
    future_offset_ms: int | None = None

    def render(self, real_now: datetime) -> str:
        """Message à envoyer ; ``real_now`` ne sert qu'au motif EVENT_TIME_FUTUR."""
        if self.value is not None:
            return self.value
        if self.future_offset_ms is None:
            raise ValueError("émission sans valeur ni décalage futur")
        message = self.transaction.to_message()
        message["event_time"] = format_event_time(
            real_now + timedelta(milliseconds=self.future_offset_ms)
        )
        return json.dumps(message, ensure_ascii=False, separators=(",", ":"))

    def to_jsonl(self) -> str:
        """Ligne du fichier ``--dry-run`` : stable (aucune heure réelle), comparée par sha256."""
        truth = (
            None
            if self.truth is None
            else {
                "anomaly_id": str(self.truth.anomaly_id),
                "anomaly_type": self.truth.anomaly_type,
                "expected_rule": self.truth.expected_rule,
            }
        )
        return json.dumps(
            {
                "emit_sim_time": format_event_time(self.emit_sim_time),
                "key": self.key,
                "value": self.value,
                "delay": self.delay,
                "invalid_reason": self.invalid_reason,
                "future_offset_ms": self.future_offset_ms,
                "truth": truth,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
