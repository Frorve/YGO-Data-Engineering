{{
    config(
        materialized = 'table',
        schema       = 'gold',
        alias        = 'dim_archetypes',
        tags         = ['gold', 'dimension']
    )
}}

/*
    Dimensión — dim_archetypes
    ──────────────────────────
    Dimensión de arquetipos de cartas Yu-Gi-Oh.
    Una fila por arquetipo con métricas agregadas calculadas en Gold.
    Usada en CU3 para analizar popularidad y demanda por arquetipo.
    Se relaciona con fact_popularity a través de archetype_sk.

    Métricas calculadas:
      - total_cartas: número de cartas del arquetipo
      - attribute_mas_comun: atributo más frecuente en el arquetipo
      - race_mas_comun: raza más frecuente en el arquetipo
      - tiene_banned: si alguna carta del arquetipo está Forbidden
      - tiene_limited: si alguna carta del arquetipo está Limited
*/

WITH cartas_por_arquetipo AS (

    SELECT
        sc.archetype_id                             AS archetype_name,
        COUNT(DISTINCT sc.card_id)                  AS total_cartas,
        -- Atributo más común usando MODE (valor más frecuente)
        MODE(sc.attribute_id)                       AS attribute_mas_comun,
        -- Raza más común
        MODE(sc.race_id)                            AS race_mas_comun
    FROM {{ ref('stg_cards') }} sc
    WHERE sc.archetype_id IS NOT NULL
    GROUP BY sc.archetype_id

),

ban_por_arquetipo AS (

    SELECT
        sc.archetype_id                             AS archetype_name,
        MAX(CASE WHEN sb.ban_tcg = 'Forbidden'
            THEN 1 ELSE 0 END)                      AS tiene_banned,
        MAX(CASE WHEN sb.ban_tcg = 'Limited'
            THEN 1 ELSE 0 END)                      AS tiene_limited
    FROM {{ ref('stg_cards') }} sc
    JOIN {{ ref('stg_card_banlists') }} sb
        ON sc.card_id = sb.card_id
    WHERE sc.archetype_id IS NOT NULL
    GROUP BY sc.archetype_id

)

SELECT
    -- ── Surrogate key ─────────────────────────────────────────────────────
    ROW_NUMBER() OVER (
        ORDER BY cpa.archetype_name
    )                                               AS archetype_sk,

    -- ── Identificación ────────────────────────────────────────────────────
    cpa.archetype_name,

    -- ── Métricas agregadas ────────────────────────────────────────────────
    cpa.total_cartas,
    cpa.attribute_mas_comun,
    cpa.race_mas_comun,

    -- ── Indicadores de ban ────────────────────────────────────────────────
    IFF(COALESCE(bpa.tiene_banned, 0) = 1, TRUE, FALSE)  AS tiene_banned,
    IFF(COALESCE(bpa.tiene_limited, 0) = 1, TRUE, FALSE) AS tiene_limited

FROM cartas_por_arquetipo cpa
LEFT JOIN ban_por_arquetipo bpa
    ON cpa.archetype_name = bpa.archetype_name