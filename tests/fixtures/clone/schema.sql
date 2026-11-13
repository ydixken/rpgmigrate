CREATE SCHEMA "M2 space";
CREATE EXTENSION citext;
CREATE TYPE "M2 space".mood AS ENUM ('calm', 'busy');
CREATE DOMAIN "M2 space".positive AS integer CHECK (VALUE > 0);
CREATE DOMAIN "M2 space".address AS citext;
CREATE TYPE "M2 space".size AS (width numeric, height numeric, label citext, feeling "M2 space".mood);
CREATE TABLE "M2 space".empty (id integer, value text);
CREATE TABLE "M2 space".reference (id integer PRIMARY KEY);
INSERT INTO "M2 space".reference VALUES (1), (2);
CREATE TABLE "M2 space".rows (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reference integer REFERENCES "M2 space".reference,
    value text,
    nullable text,
    data bytea,
    feeling "M2 space".mood,
    positive "M2 space".positive,
    numbers integer[],
    email citext,
    emails citext[],
    address "M2 space".address,
    size "M2 space".size,
    sizes "M2 space".size[],
    computed text GENERATED ALWAYS AS (value || ':generated') STORED
);
INSERT INTO "M2 space".rows (reference, value, nullable, data, feeling, positive, numbers, email, emails, address, size, sizes)
SELECT 1, 'fixture-sensitive-row-' || g, CASE WHEN g % 2 = 0 THEN NULL ELSE '' END,
       decode('00ff5c', 'hex'), 'calm', 1, ARRAY[g, NULL, g + 1],
       CASE WHEN g % 3 = 0 THEN NULL ELSE 'User' || g || '@Example.COM' END,
       ARRAY['Mixed', 'MIXED', NULL, '']::citext[], CASE WHEN g % 2 = 0 THEN 'Box ' || g END,
       ROW(g * 1.50, NULL, 'Label, "quoted" (' || g || ')', 'busy')::"M2 space".size,
       ARRAY[ROW(1.0, 2.25, 'A', 'calm'), NULL, ROW(NULL, NULL, '', NULL)]::"M2 space".size[]
FROM generate_series(1, 100) g;
CREATE INDEX rows_value ON "M2 space".rows(value);
CREATE TABLE "M2 space".wide (id integer, value text);
INSERT INTO "M2 space".wide SELECT g, repeat(md5(g::text), 256) FROM generate_series(1, 2048) g;
CREATE TABLE "M2 space".parent (id integer, value text);
CREATE TABLE "M2 space".child () INHERITS ("M2 space".parent);
INSERT INTO "M2 space".parent VALUES (1, NULL), (1, NULL);
INSERT INTO "M2 space".child VALUES (2, 'child');
CREATE TABLE "M2 space".parts (id integer, value text) PARTITION BY RANGE (id);
CREATE TABLE "M2 space".part_low PARTITION OF "M2 space".parts FOR VALUES FROM (0) TO (10);
CREATE TABLE "M2 space".part_high PARTITION OF "M2 space".parts FOR VALUES FROM (10) TO (20);
INSERT INTO "M2 space".parts VALUES (1, 'low'), (11, 'high');
CREATE VIEW "M2 space".visible AS SELECT id, value FROM "M2 space".rows;
CREATE FUNCTION "M2 space".preserve_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER preserve_row BEFORE UPDATE ON "M2 space".rows FOR EACH ROW EXECUTE FUNCTION "M2 space".preserve_row();
SELECT setval('"M2 space".rows_id_seq', 1000, true);
