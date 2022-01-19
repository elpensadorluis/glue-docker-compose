-- BLOQUE DE CAMBIO DE TIPO DE DATOS: fecha
alter table dev.feria_vigente_dev add column fecha_ date;

update dev.feria_vigente_dev
set fecha_ = cast(fecha as date);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN fecha;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN fecha_ TO fecha;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: ano
alter table dev.feria_vigente_dev add column ano_ INTEGER;

update dev.feria_vigente_dev
set ano_ = cast(ano as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN ano;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN ano_ TO ano;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: mes
alter table dev.feria_vigente_dev add column mes_ INTEGER;

update dev.feria_vigente_dev
set mes_ = cast(mes as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN mes;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN mes_ TO mes;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: fob
alter table dev.feria_vigente_dev add column fob_ INTEGER;

update dev.feria_vigente_dev
set fob_ = cast(fob as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN fob;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN fob_ TO fob;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: pp
alter table dev.feria_vigente_dev add column pp_ INTEGER;

update dev.feria_vigente_dev
set pp_ = cast(pp as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN pp;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN pp_ TO pp;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: moq
alter table dev.feria_vigente_dev add column moq_ INTEGER;

update dev.feria_vigente_dev
set moq_ = cast(moq as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN moq;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN moq_ TO moq;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: leadtime
alter table dev.feria_vigente_dev add column leadtime_ INTEGER;

update dev.feria_vigente_dev
set leadtime_ = cast(leadtime as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN leadtime;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN leadtime_ TO leadtime;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: volumen
alter table dev.feria_vigente_dev add column volumen_ INTEGER;

update dev.feria_vigente_dev
set volumen_ = cast(volumen as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN volumen;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN volumen_ TO volumen;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: peso
alter table dev.feria_vigente_dev add column peso_ INTEGER;

update dev.feria_vigente_dev
set peso_ = cast(peso as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN peso;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN peso_ TO peso;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: inner_
alter table dev.feria_vigente_dev add column inner__ INTEGER;

update dev.feria_vigente_dev
set inner__ = cast(inner_ as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN inner_;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN inner__ TO inner_;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: master
alter table dev.feria_vigente_dev add column master_ INTEGER;

update dev.feria_vigente_dev
set master_ = cast(master as INTEGER);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN master;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN master_ TO master;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: fecha_ini
alter table dev.feria_vigente_dev add column fecha_ini_ DATE;

update dev.feria_vigente_dev
set fecha_ini_ = cast(fecha_ini as DATE);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN fecha_ini;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN fecha_ini_ TO fecha_ini;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: fecha_ter
alter table dev.feria_vigente_dev add column fecha_ter_ DATE;

update dev.feria_vigente_dev
set fecha_ter_ = cast(fecha_ter as DATE);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN fecha_ter;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN fecha_ter_ TO fecha_ter;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: fecha_ini_2
alter table dev.feria_vigente_dev add column fecha_ini_2_ DATE;

update dev.feria_vigente_dev
set fecha_ini_2_ = cast(fecha_ini_2 as DATE);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN fecha_ini_2;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN fecha_ini_2_ TO fecha_ini_2;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS

-- BLOQUE DE CAMBIO DE TIPO DE DATOS: fecha_ter_2
alter table dev.feria_vigente_dev add column fecha_ter_2_ DATE;

update dev.feria_vigente_dev
set fecha_ter_2_ = cast(fecha_ter_2 as DATE);

ALTER TABLE dev.feria_vigente_dev DROP COLUMN fecha_ter_2;
ALTER TABLE dev.feria_vigente_dev RENAME COLUMN fecha_ter_2_ TO fecha_ter_2;
-- FIN BLOQUE DE CAMBIO DE TIPO DE DATOS