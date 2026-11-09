CREATE SCHEMA "M2 space";
CREATE TYPE "M2 space".mood AS ENUM ('calm', 'busy');
CREATE DOMAIN "M2 space".positive AS integer;
CREATE TABLE "M2 space".empty (id integer PRIMARY KEY, value text);
CREATE TABLE "M2 space".rows (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    value text,
    nullable text,
    data bytea,
    feeling "M2 space".mood,
    positive "M2 space".positive,
    numbers integer[]
);
INSERT INTO "M2 space".rows (value, nullable, data, feeling, positive, numbers)
SELECT 'fixture-sensitive-row-' || g, CASE WHEN g % 2 = 0 THEN NULL ELSE '' END,
       decode('00ff5c', 'hex'), 'calm', 1, ARRAY[g, NULL, g + 1]
FROM generate_series(1, 100) g;
CREATE TABLE "M2 space".wide (id integer PRIMARY KEY, value text, changed integer);
INSERT INTO "M2 space".wide SELECT g, string_agg(md5((g * 10000 + x)::text), ''), 0
FROM generate_series(1, 32) g CROSS JOIN generate_series(1, 1024) x GROUP BY g;
CREATE TABLE "M2 space".indexed (id integer NOT NULL, value text);
CREATE UNIQUE INDEX indexed_identity ON "M2 space".indexed(id);
ALTER TABLE "M2 space".indexed REPLICA IDENTITY USING INDEX indexed_identity;
INSERT INTO "M2 space".indexed VALUES (1, 'index');
CREATE TABLE "M2 space".full_rows (id integer, value text);
ALTER TABLE "M2 space".full_rows REPLICA IDENTITY FULL;
INSERT INTO "M2 space".full_rows VALUES (1, NULL), (1, NULL), (NULL, 'nullable');
CREATE TABLE "M2 space".truncated (id integer PRIMARY KEY, value text);
INSERT INTO "M2 space".truncated VALUES (1, 'truncate');
SELECT setval('"M2 space".rows_id_seq', 1000, true);
