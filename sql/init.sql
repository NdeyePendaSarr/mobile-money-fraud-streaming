-- SenPay : tables de sortie du projet (PRD v1.3 §6.2, cadrage P2 phase 0 §2.5).
-- Exécuté par l'image postgres à la création du volume (docker-entrypoint-initdb.d),
-- dans la base SENPAY_PG_DB, schéma public. Rejoué seulement après make reset.
--
-- Les listes autorisées (types, canaux, régions, identifiants) reprennent la
-- validation du schéma de l'événement v1 (cadrage §2.1 et §2.2) : une ligne qui
-- les viole signale un bug du job, pas une donnée à corriger.

\set ON_ERROR_STOP on

begin;

-- ---------------------------------------------------------------------------
-- transactions : transactions brutes valides (streaming, phase 3)
-- ---------------------------------------------------------------------------
create table transactions (
    transaction_id  uuid        primary key,
    schema_version  smallint    not null check (schema_version = 1),
    event_time      timestamptz not null,               -- temps simulé (UTC)
    sender_id       text        not null check (sender_id ~ '^U-[0-9]{6}$'),
    receiver_id     text,
    type            text        not null
        check (type in ('TRANSFER', 'PAYMENT', 'CASH_IN', 'CASH_OUT')),
    amount_xof      bigint      not null check (amount_xof between 100 and 2000000),
    region          text        not null
        check (region in ('Dakar', 'Thiès', 'Diourbel', 'Saint-Louis', 'Kaolack', 'Ziguinchor')),
    channel         text        not null check (channel in ('APP', 'USSD', 'AGENT')),
    agent_id        text        check (agent_id ~ '^A-[0-9]{4}$'),
    -- Métadonnées Kafka : ingested_at = horodatage du message (temps réel, F10)
    ingested_at     timestamptz not null,
    kafka_partition integer     not null check (kafka_partition >= 0),
    kafka_offset    bigint      not null check (kafka_offset >= 0),
    -- Un message Kafka ne donne qu'une transaction
    unique (kafka_partition, kafka_offset),
    -- Matrice type × canal (cadrage §2.2)
    check (
        (type in ('TRANSFER', 'PAYMENT') and channel in ('APP', 'USSD') and agent_id is null)
        or (type in ('CASH_IN', 'CASH_OUT') and channel = 'AGENT' and agent_id is not null)
    ),
    -- Destinataire selon le type : client, marchand ou aucun
    check (
        (type = 'TRANSFER' and receiver_id ~ '^U-[0-9]{6}$' and receiver_id <> sender_id)
        or (type = 'PAYMENT' and receiver_id ~ '^M-[0-9]{4}$')
        or (type in ('CASH_IN', 'CASH_OUT') and receiver_id is null)
    )
);

create index transactions_sender_event_idx on transactions (sender_id, event_time);

-- ---------------------------------------------------------------------------
-- transactions_staging : zone de transit d'un micro-batch (phase 3, I4).
-- Spark y écrit en JDBC (append), puis le driver fusionne dans transactions en
-- une transaction : INSERT … ON CONFLICT (transaction_id) DO NOTHING, puis
-- suppression du lot. Colonnes en text : le typage (uuid…) se fait à la fusion.
-- Pas de contrainte : les CHECK restent sur transactions (dernier filet).
-- ---------------------------------------------------------------------------
create table transactions_staging (
    batch_id        bigint      not null,
    transaction_id  text        not null,
    schema_version  smallint    not null,
    event_time      timestamptz not null,
    sender_id       text        not null,
    receiver_id     text,
    type            text        not null,
    amount_xof      bigint      not null,
    region          text        not null,
    channel         text        not null,
    agent_id        text,
    ingested_at     timestamptz not null,
    kafka_partition integer     not null,
    kafka_offset    bigint      not null
);

create index transactions_staging_batch_idx on transactions_staging (batch_id);

-- ---------------------------------------------------------------------------
-- stream_batches : bilan de chaque micro-batch du consommateur (phase 3, I5).
-- AJOUT au PRD §6.2 : compte durable des messages lus, valides, insérés et
-- invalides par motif, fait par le consommateur (comparable au générateur).
-- Écrit une seule fois (ON CONFLICT (batch_id) DO NOTHING), dans la même
-- transaction que la fusion : un micro-batch rejoué garde son bilan d'origine.
-- ---------------------------------------------------------------------------
create table stream_batches (
    batch_id             bigint      primary key check (batch_id >= 0),
    nb_lus               integer     not null check (nb_lus >= 0),
    nb_valides           integer     not null check (nb_valides >= 0),
    nb_inseres           integer     not null check (nb_inseres >= 0),
    nb_invalides         integer     not null check (nb_invalides >= 0),
    nb_invalid_by_reason jsonb       not null default '{}'
        check (jsonb_typeof(nb_invalid_by_reason) = 'object'),
    processed_at         timestamptz not null default now(),
    check (nb_lus = nb_valides + nb_invalides),
    check (nb_inseres <= nb_valides)
);

-- ---------------------------------------------------------------------------
-- kpi_minute : fenêtres fixes de 1 min sur event_time (streaming, phase 5, F18)
-- Upsert sur la clé ; pas de nb_alertes (le dashboard lit alerts).
-- ---------------------------------------------------------------------------
create table kpi_minute (
    window_start      timestamptz not null,
    region            text        not null
        check (region in ('Dakar', 'Thiès', 'Diourbel', 'Saint-Louis', 'Kaolack', 'Ziguinchor')),
    channel           text        not null check (channel in ('APP', 'USSD', 'AGENT')),
    nb_transactions   integer     not null check (nb_transactions > 0),
    montant_total_xof bigint      not null check (montant_total_xof > 0),
    primary key (window_start, region, channel)
);

-- ---------------------------------------------------------------------------
-- user_profiles : profils lus par R2 ; amorcés par le générateur, puis
-- recalculés chaque nuit simulée par le batch Spark (F15)
-- ---------------------------------------------------------------------------
create table user_profiles (
    user_id           text        primary key check (user_id ~ '^U-[0-9]{6}$'),
    median_amount_xof bigint      check (median_amount_xof > 0),  -- null : aucune sortie
    nb_tx_30j         integer     not null check (nb_tx_30j >= 0),
    home_region       text
        check (home_region in ('Dakar', 'Thiès', 'Diourbel', 'Saint-Louis', 'Kaolack', 'Ziguinchor')),
    first_seen        timestamptz,
    source            text        not null check (source in ('GENERATEUR', 'BATCH')),
    computed_at       timestamptz not null default now()
);

-- ---------------------------------------------------------------------------
-- generator_runs : une exécution du générateur (phase 2)
-- ---------------------------------------------------------------------------
create table generator_runs (
    run_id       bigint      generated always as identity primary key,
    seed         bigint      not null,
    sim_start    timestamptz not null,              -- début de la plage simulée
    sim_end      timestamptz,                       -- null tant que le run tourne
    speed        numeric     check (speed > 0),     -- null : --speed max (sans attente, H6)
    -- Configuration complète, graine et options : le run est rejouable depuis la base (H17)
    params       jsonb       not null check (jsonb_typeof(params) = 'object'),
    nb_sent      bigint      not null default 0 check (nb_sent >= 0),
    nb_late      bigint      not null default 0 check (nb_late >= 0),
    nb_very_late bigint      not null default 0 check (nb_very_late >= 0),
    nb_invalid   bigint      not null default 0 check (nb_invalid >= 0),
    -- Messages invalides par motif (H15), ex. {"JSON_ILLISIBLE": 3}
    nb_invalid_by_reason jsonb not null default '{}'
        check (jsonb_typeof(nb_invalid_by_reason) = 'object'),
    -- Nombre d'épisodes par type d'anomalie, ex. {"RAFALE": 12, "MONTANT_ANORMAL": 30}
    nb_episodes  jsonb       not null default '{}' check (jsonb_typeof(nb_episodes) = 'object'),
    started_at   timestamptz not null default now(),  -- temps réel
    ended_at     timestamptz,
    check (sim_end is null or sim_end >= sim_start)
);

-- =====================================================================
-- alerts : alertes émises par le job de streaming (R1 à R3)
-- Idempotence (F16) : alert_id déterministe, donc un rejeu de
-- micro-batch retombe sur la même ligne au lieu d'en créer une autre.
-- =====================================================================
CREATE TABLE alerts (
    -- sha2(256) en hexadécimal minuscule, tel que produit par Spark :
    -- 'R2|' || transaction_id  ou  'R1|' || sender_id || '|' || window_start
    alert_id        char(64)    PRIMARY KEY
                    CHECK (alert_id ~ '^[0-9a-f]{64}$'),
    rule_id         text        NOT NULL
                    CHECK (rule_id IN ('R1', 'R2', 'R3')),   -- R4/R5 : bonus, à ajouter alors
    -- Pas de clé étrangère vers transactions : dans un même micro-batch,
    -- l'alerte peut être écrite avant la transaction (deux écritures distinctes).
    transaction_id  uuid,
    sender_id       text        NOT NULL
                    CHECK (sender_id ~ '^U-[0-9]{6}$'),
    severity        text        NOT NULL
                    CHECK (severity IN ('MOYENNE', 'HAUTE')),
    event_time      timestamptz,                -- R2, R3 : temps simulé de la transaction
    window_start    timestamptz,                -- R1 : fenêtre glissante
    window_end      timestamptz,
    ingested_at     timestamptz NOT NULL,       -- horodatage Kafka (base de la latence, F10)
    details         jsonb       NOT NULL DEFAULT '{}'::jsonb
                    CHECK (jsonb_typeof(details) = 'object'),
    detected_at     timestamptz NOT NULL DEFAULT now(),  -- première détection, jamais modifiée
    last_updated_at timestamptz,                -- R1 seulement : dernière croissance du compteur

    -- R1 porte sur une fenêtre ; R2 et R3 sur une transaction.
    CONSTRAINT alerts_forme_par_regle CHECK (
        (rule_id = 'R1'
            AND transaction_id IS NULL AND event_time IS NULL
            AND window_start IS NOT NULL AND window_end > window_start)
        OR
        (rule_id IN ('R2', 'R3')
            AND transaction_id IS NOT NULL AND event_time IS NOT NULL
            AND window_start IS NULL AND window_end IS NULL
            AND last_updated_at IS NULL)
    ),
    -- L'upsert monotone de R1 compare details->>'nb_transferts' :
    -- si la clé manquait, la comparaison donnerait NULL et la mise à jour
    -- serait ignorée sans erreur. On l'interdit à la source.
    CONSTRAINT alerts_r1_compteur CHECK (
        rule_id <> 'R1'
        OR (details ? 'nb_transferts'
            AND jsonb_typeof(details -> 'nb_transferts') = 'number')
    )
);

-- detected_at sert à mesurer la latence : on garantit en base
-- qu'aucune mise à jour (rejeu, upsert R1) ne peut le réécrire.
CREATE FUNCTION alerts_detected_at_immuable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.detected_at IS DISTINCT FROM OLD.detected_at THEN
        RAISE EXCEPTION 'alerts.detected_at est immuable (alert_id=%)', OLD.alert_id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER alerts_detected_at_immuable
    BEFORE UPDATE ON alerts
    FOR EACH ROW EXECUTE FUNCTION alerts_detected_at_immuable();

CREATE INDEX alerts_detected_at_idx ON alerts (detected_at);   -- Grafana : alertes récentes
CREATE INDEX alerts_sender_idx      ON alerts (sender_id);      -- batch : vrai positif R1


-- =====================================================================
-- injected_anomalies : vérité terrain écrite par le générateur (F1).
-- Jamais lue par les règles ; seulement par le rapport de détection.
-- Une ligne = une transaction d'un épisode (une rafale = 6 à 10 lignes).
-- =====================================================================
CREATE TABLE injected_anomalies (
    anomaly_id      uuid        NOT NULL,       -- l'épisode
    -- Pas de clé étrangère vers transactions : le générateur écrit la vérité
    -- terrain avant que Spark n'écrive la transaction (et une transaction
    -- très en retard peut ne jamais y arriver côté streaming).
    transaction_id  uuid        NOT NULL,
    run_id          bigint      NOT NULL REFERENCES generator_runs (run_id),
    anomaly_type    text        NOT NULL,
    expected_rule   text        NOT NULL,
    sender_id       text        NOT NULL
                    CHECK (sender_id ~ '^U-[0-9]{6}$'),
    event_time      timestamptz NOT NULL,       -- temps simulé de la transaction

    PRIMARY KEY (anomaly_id, transaction_id),
    -- Une transaction appartient à un seul épisode.
    CONSTRAINT injected_anomalies_tx_unique UNIQUE (transaction_id),
    -- Le couple type → règle attendue est fixé par le cadrage (§4).
    CONSTRAINT injected_anomalies_type_regle CHECK (
        (anomaly_type, expected_rule) IN (
            ('RAFALE',            'R1'),
            ('MONTANT_ANORMAL',   'R2'),
            ('NUIT_GROS_MONTANT', 'R3'),
            ('CASH_OUT_RAPIDE',   'R4'),
            ('CHANGEMENT_REGION', 'R5')
        )
    )
);

CREATE INDEX injected_anomalies_type_idx ON injected_anomalies (anomaly_type);

-- ---------------------------------------------------------------------------
-- daily_kpi : agrégats quotidiens par région et canal (batch Spark, phase 7)
-- ---------------------------------------------------------------------------
create table daily_kpi (
    day               date        not null,          -- date simulée (Africa/Dakar)
    region            text        not null
        check (region in ('Dakar', 'Thiès', 'Diourbel', 'Saint-Louis', 'Kaolack', 'Ziguinchor')),
    channel           text        not null check (channel in ('APP', 'USSD', 'AGENT')),
    nb_transactions   bigint      not null check (nb_transactions >= 0),
    montant_total_xof bigint      not null check (montant_total_xof >= 0),
    nb_alertes        integer     not null check (nb_alertes >= 0),
    computed_at       timestamptz not null default now(),
    primary key (day, region, channel)
);

-- ---------------------------------------------------------------------------
-- detection_report : qualité de la détection par jour et par règle
-- (batch Spark, phase 7 ; définitions PRD §7.2). rule_id = 'GLOBAL' pour la
-- ligne toutes règles confondues.
-- ---------------------------------------------------------------------------
create table detection_report (
    day                  date         not null,
    rule_id              text         not null
        check (rule_id in ('R1', 'R2', 'R3', 'R4', 'R5', 'GLOBAL')),
    nb_episodes_injected integer      not null check (nb_episodes_injected >= 0),
    nb_episodes_detected integer      not null check (nb_episodes_detected >= 0),
    nb_alerts            integer      not null check (nb_alerts >= 0),
    nb_false_positives   integer      not null check (nb_false_positives >= 0),
    precision            numeric(5,4) check (precision between 0 and 1),  -- null : aucune alerte
    recall               numeric(5,4) check (recall between 0 and 1),     -- null : aucun épisode
    latency_median_s     numeric(10,3) check (latency_median_s >= 0),
    latency_p95_s        numeric(10,3) check (latency_p95_s >= 0),
    latency_max_s        numeric(10,3) check (latency_max_s >= 0),
    computed_at          timestamptz  not null default now(),
    primary key (day, rule_id),
    check (nb_episodes_detected <= nb_episodes_injected),
    check (nb_false_positives <= nb_alerts)
);

commit;
