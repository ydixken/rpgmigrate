DO $fixture$
DECLARE
    shape text := pg_catalog.current_setting('rpgmigrate.bench_shape');
BEGIN
    IF shape = 'narrow' THEN
        FOR i IN 1..16 LOOP
            EXECUTE pg_catalog.format(
                'INSERT INTO m9.data_%s SELECT r, r * 7, md5(%L || r::text || '':a'') || md5(%L || r::text || '':b'') || md5(%L || r::text || '':c'') FROM generate_series(1,65536) r',
                i, i::text || ':', i::text || ':', i::text || ':');
        END LOOP;
    ELSIF shape = 'wide' THEN
        FOR i IN 1..4 LOOP
            EXECUTE pg_catalog.format(
                'INSERT INTO m9.data_%s SELECT r, r * 7, (SELECT string_agg(md5(%L || r::text || '':'' || chunk::text), '''' ORDER BY chunk) FROM generate_series(1,256) chunk) FROM generate_series(1,4096) r',
                i, i::text || ':');
        END LOOP;
    ELSE
        RAISE EXCEPTION 'unsupported benchmark fixture';
    END IF;
END
$fixture$;
INSERT INTO m9.probe VALUES (1, 0);
ANALYZE;
