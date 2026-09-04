-- Reliar outbox table (SRS §24, §24.1; ADRs 0008, 0009, 0012, 0016, 0023).
--
-- Runs with `search_path` already resolved to the configured schema by `migrate()`
-- (ADR 0018) — every identifier here is deliberately unqualified and unprefixed (ADR 0017).
--
-- Forward-only: this file is never edited once applied (ADR 0018). PostgreSQL 18+ is the
-- minimum supported version (`uuidv7()`, identity columns on partitioned tables).

CREATE TABLE outbox (
    -- message identity
    id                  uuid NOT NULL DEFAULT uuidv7(),
    -- monotonic ordering tiebreak; a random uuid PK and a tying created_at cannot order rows
    -- (§22.2). Not gap-free.
    sequence            bigint GENERATED ALWAYS AS IDENTITY,
    message_type        text NOT NULL,
    message_version     integer NOT NULL,

    -- promoted correlation metadata (§24.2)
    correlation_id      text,
    conversation_id     uuid NOT NULL,
    causation_id        uuid,
    request_id          uuid,

    -- serialized message
    content_type        text NOT NULL,
    payload             bytea NOT NULL,

    -- promoted metadata (§24.2)
    tenant_id           text,
    expires_at          timestamptz,

    -- ordering-strategy support (§22.2); NULL = unordered
    ordering_key        text,

    -- extensible data: the MetadataRest remainder (§24.2) and the caller's custom headers,
    -- never merged with each other or with a promoted column
    metadata            jsonb,
    headers             jsonb,

    -- shape marker for the metadata JSONB remainder (§24.2); v0.1 writes 1
    metadata_version    integer NOT NULL DEFAULT 1,

    -- outbox processing state
    created_at          timestamptz NOT NULL DEFAULT now(),
    available_at        timestamptz NOT NULL DEFAULT now(),
    -- last state transition, for operators and retention diagnostics
    updated_at          timestamptz NOT NULL DEFAULT now(),

    attempts            integer NOT NULL DEFAULT 0,
    locked_by           text,
    locked_until        timestamptz,

    published_at        timestamptz,
    dead_at             timestamptz,
    -- why the row died (§19.1's DeadReason), set with dead_at and never alone (ADR 0023)
    dead_reason         text,

    last_error          text
);

ALTER TABLE outbox
    ADD CONSTRAINT pk_outbox                  PRIMARY KEY (id),
    ADD CONSTRAINT ck_outbox_attempts         CHECK (attempts >= 0),
    ADD CONSTRAINT ck_outbox_message_version  CHECK (message_version >= 0),
    ADD CONSTRAINT ck_outbox_metadata_version CHECK (metadata_version >= 1),
    ADD CONSTRAINT ck_outbox_lease            CHECK ((locked_by IS NULL) = (locked_until IS NULL)),
    ADD CONSTRAINT ck_outbox_terminal         CHECK (published_at IS NULL OR dead_at IS NULL),
    ADD CONSTRAINT ck_outbox_dead_reason      CHECK ((dead_at IS NULL) = (dead_reason IS NULL));

-- the identity column's monotonic tiebreak (§22.2); a unique INDEX, not a UNIQUE constraint —
-- every index is `ix_`, unique or not (decision 27)
CREATE UNIQUE INDEX ix_outbox_sequence ON outbox (sequence);

-- the claim query (canonical form fixed in §24.1)
CREATE INDEX ix_outbox_pending ON outbox (available_at, sequence)
    WHERE published_at IS NULL AND dead_at IS NULL;

-- retention purge of published rows
CREATE INDEX ix_outbox_published ON outbox (published_at)
    WHERE published_at IS NOT NULL;

-- dead-letter listing / keyset pagination on `sequence` (§19.3); the INCLUDE evaluates
-- `dead_before` without a heap fetch when message_type/tenant_id are unset
CREATE INDEX ix_outbox_dead ON outbox (sequence) INCLUDE (dead_at)
    WHERE dead_at IS NOT NULL;

-- retention delete of dead rows past `dead_retention` (§23.2) — the opposite access pattern
CREATE INDEX ix_outbox_dead_at ON outbox (dead_at)
    WHERE dead_at IS NOT NULL;

-- Ordering::PerKey's claim predicate (§22.2); PerKey ships in 0.2 but the index costs nothing now
CREATE INDEX ix_outbox_ordering_key ON outbox (ordering_key, sequence)
    WHERE ordering_key IS NOT NULL AND published_at IS NULL AND dead_at IS NULL;

-- the expiry sweep (§12.2)
CREATE INDEX ix_outbox_expires ON outbox (expires_at)
    WHERE expires_at IS NOT NULL AND published_at IS NULL AND dead_at IS NULL;
