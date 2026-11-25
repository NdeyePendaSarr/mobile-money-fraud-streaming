"""Livraison des transactions : retards d'émission (H14) et messages invalides (H15).

Entrée : les transactions du run, dans l'ordre de ``event_time``, tranche horaire
par tranche horaire. Sortie : les émissions, dans l'ordre de ``emit_sim_time``.

- **Retards** : 5 % des messages partent 10 s à 5 min (simulées) après leur
  ``event_time``, 0,2 % de 15 à 30 min. ``event_time`` n'est jamais modifié : un
  message « en retard » part après d'autres plus récents, ce que le watermark
  devra gérer. Les messages en attente sont gardés dans un tas (``heapq``) et
  sortent à leur instant d'émission (H2). Comme une transaction plus tardive a
  un ``event_time`` ≥ fin de tranche, tout ce qui sort avant la fin de la
  tranche est définitif : l'ordre global reste trié.
- **Messages invalides** : 0,5 % des transactions **normales** sont remplacées par
  une version corrompue, avec un motif parmi 8 (une règle de validation chacun).
- **Épisodes** (H12) : jamais invalides, jamais très en retard ; le retard
  ordinaire reste possible (il teste le watermark pour de vrai).

Deux flux aléatoires à part (``{seed}:retards``, ``{seed}:invalides``) : ni le
trafic ni les épisodes ne changent quand on modifie ces taux.
"""

from __future__ import annotations

import heapq
import json
import random
from dataclasses import dataclass, field

from producer.config import GeneratorConfig
from producer.events import Emission, Transaction, Truth, from_ms, to_ms

# Motifs d'invalidité (H15), dans un ordre fixe : chacun viole une règle du schéma v1
INVALID_REASONS: tuple[str, ...] = (
    "JSON_ILLISIBLE",
    "CHAMP_MANQUANT",
    "SCHEMA_VERSION",
    "TYPE_INCONNU",
    "MONTANT_HORS_BORNES",
    "CANAL_INCOHERENT",
    "RECEVEUR_EGAL_EMETTEUR",
    "EVENT_TIME_FUTUR",
)

# Champs toujours obligatoires (receiver_id et agent_id dépendent du type)
_CHAMPS_OBLIGATOIRES = (
    "schema_version",
    "transaction_id",
    "event_time",
    "sender_id",
    "type",
    "amount_xof",
    "region",
    "channel",
)
_TYPES_INCONNUS = ("REFUND", "DEPOSIT", "WITHDRAWAL", "TRANSFERT")


@dataclass
class DeliveryCounters:
    """Compteurs du run (``generator_runs``)."""

    nb_late: int = 0
    nb_very_late: int = 0
    nb_invalid: int = 0
    nb_invalid_by_reason: dict[str, int] = field(default_factory=dict)


def _dumps(message: dict[str, object]) -> str:
    return json.dumps(message, ensure_ascii=False, separators=(",", ":"))


def corrupt(rng: random.Random, tx: Transaction, reason: str) -> str | None:
    """Version invalide de ``tx`` pour ``reason`` ; une seule règle est violée.

    Renvoie ``None`` pour EVENT_TIME_FUTUR : la valeur est construite à l'envoi.
    """
    message = tx.to_message()
    if reason == "JSON_ILLISIBLE":
        texte = _dumps(message)
        # Tout préfixe strict d'un objet JSON est illisible (accolade fermante absente)
        return texte[: rng.randint(1, len(texte) - 1)]
    if reason == "CHAMP_MANQUANT":
        del message[rng.choice(_CHAMPS_OBLIGATOIRES)]
    elif reason == "SCHEMA_VERSION":
        message["schema_version"] = 2
    elif reason == "TYPE_INCONNU":
        message["type"] = rng.choice(_TYPES_INCONNUS)
    elif reason == "MONTANT_HORS_BORNES":
        message["amount_xof"] = rng.choice(
            (rng.randint(-1_000, 99), rng.randint(2_000_001, 10_000_000))
        )
    elif reason == "CANAL_INCOHERENT":
        # Un transfert « par agent », ou un dépôt « par l'application »
        if tx.type in ("TRANSFER", "PAYMENT"):
            message["channel"] = "AGENT"
        else:
            message["channel"] = rng.choice(("APP", "USSD"))
    elif reason == "RECEVEUR_EGAL_EMETTEUR":
        # Transfert vers soi-même (le reste du message reste un TRANSFER valide)
        message["type"] = "TRANSFER"
        if message["channel"] not in ("APP", "USSD"):
            message["channel"] = rng.choice(("APP", "USSD"))
        message["agent_id"] = None
        message["receiver_id"] = tx.sender_id
    elif reason == "EVENT_TIME_FUTUR":
        return None
    else:
        raise ValueError(f"motif inconnu : {reason}")
    return _dumps(message)


class Delivery:
    """Transforme le flux de transactions en émissions (retards, invalides)."""

    def __init__(self, config: GeneratorConfig, seed: int) -> None:
        self._config = config
        self._delay_rng = random.Random(f"{seed}:retards")
        self._invalid_rng = random.Random(f"{seed}:invalides")
        self._heap: list[tuple[int, int, Emission]] = []
        self._sequence = 0  # départage deux émissions au même instant (ordre d'arrivée)
        self.counters = DeliveryCounters(nb_invalid_by_reason=dict.fromkeys(INVALID_REASONS, 0))

    def push(self, tx: Transaction, truth: Truth | None) -> None:
        """Ajoute une transaction (dans l'ordre de ``event_time``)."""
        value: str | None = tx.to_json()
        reason: str | None = None
        future_offset_ms: int | None = None

        # Messages invalides : un tirage par transaction normale seulement (H12)
        if truth is None and self._invalid_rng.random() < self._config.invalid.share:
            reason = self._invalid_rng.choice(INVALID_REASONS)
            value = corrupt(self._invalid_rng, tx, reason)
            if reason == "EVENT_TIME_FUTUR":
                inv = self._config.invalid
                future_offset_ms = self._invalid_rng.randint(
                    inv.future_offset_min_s * 1000, inv.future_offset_max_s * 1000
                )
            self.counters.nb_invalid += 1
            self.counters.nb_invalid_by_reason[reason] += 1

        # Retards : un tirage par transaction, épisodes compris
        d = self._config.delays
        u = self._delay_rng.random()
        delay: str | None = None
        delay_ms = 0
        if u < d.very_late_share:
            if truth is None:  # jamais très en retard sur un épisode (H12)
                delay = "VERY_LATE"
                delay_ms = self._delay_rng.randint(
                    d.very_late_min_s * 1000, d.very_late_max_s * 1000
                )
        elif u < d.very_late_share + d.late_share:
            delay = "LATE"
            delay_ms = self._delay_rng.randint(d.late_min_s * 1000, d.late_max_s * 1000)
        if delay == "LATE":
            self.counters.nb_late += 1
        elif delay == "VERY_LATE":
            self.counters.nb_very_late += 1

        emit_ms = to_ms(tx.event_time) + delay_ms
        emission = Emission(
            emit_sim_time=from_ms(emit_ms),
            key=tx.sender_id,
            value=value,
            transaction=tx,
            truth=truth,
            delay=delay,
            invalid_reason=reason,
            future_offset_ms=future_offset_ms,
        )
        heapq.heappush(self._heap, (emit_ms, self._sequence, emission))
        self._sequence += 1

    def pop_before(self, limit_ms: int) -> list[Emission]:
        """Émissions dont l'instant d'émission est < ``limit_ms``, dans l'ordre."""
        sorties: list[Emission] = []
        while self._heap and self._heap[0][0] < limit_ms:
            sorties.append(heapq.heappop(self._heap)[2])
        return sorties

    def drain(self) -> list[Emission]:
        """Toutes les émissions restantes : les retardataires après la fin de la plage."""
        sorties = [item[2] for item in sorted(self._heap)]
        self._heap.clear()
        return sorties
