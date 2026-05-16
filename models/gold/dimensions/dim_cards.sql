{{
    config(
        materialized = 'table',
        schema       = 'gold',
        alias        = 'dim_cards',
        tags         = ['gold', 'dimension']
    )
}}

/*
    Dimensión — dim_cards
    ─────────────────────
    Dimensión principal de cartas del universo Yu-Gi-Oh.
    Una fila por carta única con sus atributos estables.
    Enriquecida con métricas calculadas en Gold:
      - total_sets: número de sets en los que aparece la carta
      - total_artworks: número de artworks disponibles
      - image_url: imagen principal para visualización
    Es la dimensión central del modelo, todas las facts la referencian.
*/

WITH cartas AS (

    SELECT DISTINCT
        card_id,
        card_name,
        card_type_id        AS card_type,
        race_id             AS race,
        attribute_id        AS attribute,
        archetype_id        AS archetype,
        atk,
        def,
        level,
        has_effect,
        is_monster,
        is_spell,
        is_trap,
        is_extra_deck,
        tcg_date,
        ocg_date
    FROM {{ ref('stg_cards') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY card_id
        ORDER BY ingesta_ts DESC
    ) = 1

),

total_sets AS (

    SELECT
        card_id,
        COUNT(DISTINCT set_code)            AS total_sets
    FROM {{ ref('stg_card_sets') }}
    GROUP BY card_id

),

total_artworks AS (

    SELECT
        card_id,
        COUNT(DISTINCT image_id)            AS total_artworks
    FROM {{ ref('stg_card_images') }}
    GROUP BY card_id

),

imagen_principal AS (

    SELECT
        card_id,
        image_url
    FROM {{ ref('stg_card_images') }}
    WHERE is_main_artwork = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY card_id
        ORDER BY ingesta_ts DESC
    ) = 1

),


SELECT
    -- ── Surrogate key ─────────────────────────────────────────────────────
    ROW_NUMBER() OVER (ORDER BY c.card_id)          AS card_sk,

    -- ── Identificación ────────────────────────────────────────────────────
    c.card_id,
    c.card_name,

    -- ── Clasificación ────────────────────────────────────────────────────
    c.card_type,
    c.race,
    c.attribute,
    c.archetype,

    -- ── Estadísticas de combate ───────────────────────────────────────────
    c.atk,
    c.def,
    c.level,
    c.has_effect,

    -- ── Clasificadores ────────────────────────────────────────────────────
    c.is_monster,
    c.is_spell,
    c.is_trap,
    c.is_extra_deck,

    -- ── Fechas de lanzamiento ─────────────────────────────────────────────
    c.tcg_date,
    c.ocg_date,

    -- ── Métricas enriquecidas ─────────────────────────────────────────────
    COALESCE(ts.total_sets, 0)                      AS total_sets,
    COALESCE(ta.total_artworks, 0)                  AS total_artworks,
    ip.image_url

FROM cartas c
LEFT JOIN total_sets ts
    ON c.card_id = ts.card_id
LEFT JOIN total_artworks ta
    ON c.card_id = ta.card_id
LEFT JOIN imagen_principal ip
    ON c.card_id = ip.card_id