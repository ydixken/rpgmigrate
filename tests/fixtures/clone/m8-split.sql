CREATE TABLE public.m8_split(id bigint PRIMARY KEY,data bytea);
ALTER TABLE public.m8_split ALTER COLUMN data SET STORAGE PLAIN;
INSERT INTO public.m8_split SELECT g,decode(repeat(md5(g::text),118)||substr(md5(g::text),1,24),'hex') FROM generate_series(1,286720) g;
