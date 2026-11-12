CREATE TABLE public.m8_fallback(id integer PRIMARY KEY,value text,tags integer[]);
INSERT INTO public.m8_fallback VALUES(1,NULL,ARRAY[1,NULL,2]),(2,'',ARRAY[]::integer[]),(3,'fallback',NULL);
