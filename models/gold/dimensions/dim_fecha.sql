{{
    config(
        materialized = 'table',
        schema       = 'gold',
        alias        = 'dim_fecha',
        tags         = ['gold', 'dimension']
    )
}}

/*
    Dimensión — dim_fecha
    ─────────────────────
    Dimensión de fechas generada sintéticamente.
    Cubre desde 2002-01-01 (primer lanzamiento TCG de Yu-Gi-Oh)
    hasta 2030-12-31.
    No tiene dependencias de Bronze ni Silver.
    Usada en todas las tablas de hechos para análisis temporal.

    fecha_sk se genera en formato YYYYMMDD para facilitar
    los joins desde las facts usando fechas truncadas.
*/

WITH fechas AS (

    SELECT
        DATEADD(
            day,
            seq4(),
            '2002-01-01'::DATE
        ) AS fecha_date
    FROM TABLE(GENERATOR(ROWCOUNT => 10593))  -- días entre 2002-01-01 y 2030-12-31

)

SELECT
    -- ── Surrogate key ─────────────────────────────────────────────────────
    TO_NUMBER(TO_CHAR(fecha_date, 'YYYYMMDD'))      AS fecha_sk,

    -- ── Fecha completa ────────────────────────────────────────────────────
    fecha_date,

    -- ── Atributos temporales ──────────────────────────────────────────────
    YEAR(fecha_date)                                AS anio,
    QUARTER(fecha_date)                             AS trimestre,
    MONTH(fecha_date)                               AS mes,
    MONTHNAME(fecha_date)                           AS mes_nombre,
    WEEKOFYEAR(fecha_date)                          AS semana,
    DAYOFWEEK(fecha_date)                           AS dia_semana,
    DAYNAME(fecha_date)                             AS dia_nombre,
    DAY(fecha_date)                                 AS dia,

    -- ── Indicadores ───────────────────────────────────────────────────────
    IFF(DAYOFWEEK(fecha_date) IN (0, 6), TRUE, FALSE) AS es_fin_de_semana

FROM fechas
WHERE fecha_date <= '2030-12-31'::DATE
ORDER BY fecha_date