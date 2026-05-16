{{
    config(
        materialized = 'table',
        schema       = 'gold',
        alias        = 'dim_sets',
        tags         = ['gold', 'dimension']
    )
}}

/*
    Dimensión — dim_sets
    ────────────────────
    Dimensión de sets de cartas Yu-Gi-Oh.
    Una fila por cada combinación única de carta, set y rareza.
    Usada en CU4 para analizar el valor de la colección
    agrupado por set y rareza.
    Se relaciona con dim_cards a través de card_id.
*/

SELECT
    -- ── Surrogate key ─────────────────────────────────────────────────────
    ROW_NUMBER() OVER (
        ORDER BY card_id, set_code, set_rarity
    )                                               AS set_sk,

    -- ── Identificación ────────────────────────────────────────────────────
    card_id,
    card_name,

    -- ── Información del set ───────────────────────────────────────────────
    set_name,
    set_code,
    set_rarity,
    set_rarity_code,
    set_price,
    is_high_rarity

FROM (
    SELECT DISTINCT
        card_id,
        card_name,
        set_name,
        set_code,
        set_rarity,
        set_rarity_code,
        set_price,
        is_high_rarity
    FROM {{ ref('stg_card_sets') }}
)