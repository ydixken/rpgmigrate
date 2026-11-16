-- The operator's legacy.sql objects that follow admits since M8, applied on top of schema.sql.
-- tools/e2e/widening.py adds the major-dependent generated columns.
CREATE SCHEMA widening;
CREATE DOMAIN widening.percent AS numeric CHECK (VALUE >= 0 AND VALUE <= 100);
CREATE TABLE widening.users (
    id integer PRIMARY KEY,
    email citext NOT NULL,
    display_name text NOT NULL,
    quota widening.percent NOT NULL,
    CONSTRAINT users_email_key UNIQUE (email)
);
CREATE INDEX users_name_lower_idx ON widening.users (lower(display_name));
INSERT INTO widening.users VALUES (1, 'Alice@Example.COM', 'Alice', 10), (2, 'bob@example.com', 'Bob', 20);
CREATE TABLE widening.readings (
    id integer PRIMARY KEY,
    unit_price numeric(10,4) NOT NULL,
    quantity integer NOT NULL,
    captured_at date NOT NULL,
    total_price numeric(14,4) GENERATED ALWAYS AS (unit_price * quantity) STORED
);
CREATE INDEX readings_recent_partial_idx ON widening.readings (captured_at) WHERE quantity > 5;
INSERT INTO widening.readings (id, unit_price, quantity, captured_at) VALUES (1, 1.25, 2, '2026-01-01'), (2, 3.5, 7, '2026-01-02');
CREATE TABLE widening.events (id integer PRIMARY KEY, occurred_on date NOT NULL, severity text NOT NULL);
INSERT INTO widening.events VALUES (1, '2026-01-01', 'info'), (2, '2026-01-01', 'info'), (3, '2026-01-02', 'warn');
CREATE MATERIALIZED VIEW widening.event_counts AS
    SELECT occurred_on AS day, severity, count(*) AS n FROM widening.events GROUP BY 1, 2;
