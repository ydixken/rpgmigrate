CREATE SCHEMA "E1 space";
CREATE TABLE "E1 space"."row ""payload" (
    bucket integer,
    "text value" text,
    bytes bytea,
    wide text,
    calendar date,
    elapsed interval,
    instant timestamp with time zone,
    wall timestamp without time zone,
    floating double precision,
    exact numeric(20,6),
    flag boolean
);
CREATE TABLE "E1 space"."empty table" ("nullable value" text, bytes bytea);
