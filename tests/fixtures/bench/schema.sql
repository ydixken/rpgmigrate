SELECT pg_catalog.set_config('rpgmigrate.bench_shape', :'shape', false);
CREATE SCHEMA m9;
DO $fixture$
DECLARE
    shape text := pg_catalog.current_setting('rpgmigrate.bench_shape');
    table_count integer;
BEGIN
    IF shape NOT IN ('narrow', 'wide') THEN
        RAISE EXCEPTION 'unsupported benchmark fixture';
    END IF;
    table_count := CASE shape WHEN 'narrow' THEN 16 ELSE 4 END;
    FOR i IN 1..table_count LOOP
        EXECUTE pg_catalog.format('CREATE TABLE m9.data_%s (id bigint NOT NULL, value bigint NOT NULL, payload text NOT NULL)', i);
    END LOOP;
END
$fixture$;
CREATE TABLE m9.probe (id bigint PRIMARY KEY, value bigint NOT NULL);
