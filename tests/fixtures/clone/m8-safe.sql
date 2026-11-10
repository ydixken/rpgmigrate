CREATE TABLE public.m8_safe(id bigint PRIMARY KEY,value text,data bytea);
INSERT INTO public.m8_safe VALUES(1,NULL,decode('00ff275c','hex')),(2,'',NULL),(3,'safe',decode('','hex')),(4,'safe',decode('00ff275c','hex'));
