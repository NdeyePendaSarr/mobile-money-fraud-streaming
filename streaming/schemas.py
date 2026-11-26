"""Schéma de l'événement « transaction » v1, côté Spark (cadrage phase 0, §2.1).

Aucun champ de vérité terrain (F1) : le schéma lu par Spark ne contient ni type
d'anomalie ni règle attendue.

Deux lectures du même JSON (voir ``streaming.validation``) :

- ``RAW_SCHEMA`` : **tous les champs en texte**, plus la colonne de l'enregistrement
  corrompu. Un champ texte accepte n'importe quelle valeur JSON (nombre, booléen,
  objet), donc seul un JSON **illisible** remplit ``_corrupt_record`` : un champ
  « mal typé » ne peut pas être pris pour un JSON illisible ;
- ``TYPED_SCHEMA`` : les deux champs entiers typés. Une valeur JSON du mauvais type
  (texte, décimal, booléen) y donne ``null``, ce qui distingue « 1 » de 1.
"""

from __future__ import annotations

from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

CORRUPT_COLUMN = "_corrupt_record"

# Les 10 champs du schéma v1, dans l'ordre du message
FIELDS: tuple[str, ...] = (
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

RAW_SCHEMA = StructType(
    [StructField(name, StringType(), nullable=True) for name in FIELDS]
    + [StructField(CORRUPT_COLUMN, StringType(), nullable=True)]
)

TYPED_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType(), nullable=True),
        StructField("amount_xof", LongType(), nullable=True),
    ]
)

# Options de from_json : mode permissif, l'enregistrement illisible va dans la colonne
RAW_OPTIONS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COLUMN}

# Énumérations (cadrage phase 0, §2.1 et §2.2)
TYPES = ("TRANSFER", "PAYMENT", "CASH_IN", "CASH_OUT")
REGIONS = ("Dakar", "Thiès", "Diourbel", "Saint-Louis", "Kaolack", "Ziguinchor")

# Formats (expressions régulières Java, ancrées)
UUID_V4 = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
EVENT_TIME = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"
CLIENT = r"^U-\d{6}$"
MARCHAND = r"^M-\d{4}$"
AGENT = r"^A-\d{4}$"
EVENT_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
