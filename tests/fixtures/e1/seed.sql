-- Paired bucket values make whole-row duplicates visible to the digest oracle.
INSERT INTO "E1 space"."row ""payload"
SELECT bucket,
    CASE bucket % 6 WHEN 0 THEN NULL WHEN 1 THEN '' WHEN 2 THEN E'line\n tab\t slash\\'
         WHEN 3 THEN 'Grüße 日本語 🐘' WHEN 4 THEN E'\\N' ELSE 'same value' END,
    CASE bucket % 4 WHEN 0 THEN NULL WHEN 1 THEN ''::bytea
         ELSE decode('000a0d5c7fff' || lpad(to_hex(bucket), 8, '0'), 'hex') END,
    CASE WHEN bucket=1000 THEN repeat(md5('e1-wide-seed'),294913)
         ELSE repeat(md5(bucket::text),16) END,
    DATE '2001-02-03' + (bucket % 31),
    make_interval(months => -(bucket % 13), days => bucket % 29, secs => -1.250001),
    TIMESTAMPTZ '2001-02-03 04:05:06.123456+05:45' + bucket * INTERVAL '1 second',
    TIMESTAMP '2001-02-03 04:05:06.123456' + bucket * INTERVAL '1 second',
    CASE bucket % 7 WHEN 0 THEN 'NaN'::float8 WHEN 1 THEN 'Infinity'::float8
         WHEN 2 THEN '-Infinity'::float8 WHEN 3 THEN '-0'::float8
         WHEN 4 THEN 1.2345678901234567 WHEN 5 THEN 1e-300 ELSE 1e300 END,
    (bucket - 2500)::numeric / 1000000,
    CASE bucket % 3 WHEN 0 THEN NULL WHEN 1 THEN true ELSE false END
FROM (SELECT i / 2 AS bucket FROM generate_series(0,10002) AS s(i)) AS paired
ORDER BY bucket;
