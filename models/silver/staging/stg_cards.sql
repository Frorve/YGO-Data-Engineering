{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'stg_cards',
        tags         = ['silver', 'staging']
    )
}}

/*
    Staging — stg_cards
    ───────────────────
    Limpia y tipifica los campos de bronze_flat_cards con evidencia
    de uso en los casos de uso actuales.
    Solo incluye campos que alimentan directamente CU1, CU2, CU3 o CU4.
    Se reconstruye completa en cada ejecución.
*/

SELECT
    -- ── Metadatos de trazabilidad ─────────────────────────────────────────
    card_id,
    ingesta_id,
    ingesta_ts,
    es_mock,

    -- ── Identificación ────────────────────────────────────────────────────
    TRIM(card_name)                                     AS card_name,

    -- ── Clasificación (FKs hacia tablas ref_) ────────────────────────────
    card_type                                           AS card_type_id,
    race                                                AS race_id,
    attribute                                           AS attribute_id,
    archetype                                           AS archetype_id,

    -- ── Estadísticas de combate ───────────────────────────────────────────
    atk,
    def,
    level,

    -- ── Popularidad y demanda (CU3) ───────────────────────────────────────
    views,
    viewsweek,
    upvotes,
    downvotes,

    -- ── Fechas de lanzamiento ─────────────────────────────────────────────
    tcg_date,
    ocg_date,

    -- ── Efecto ───────────────────────────────────────────────────────────
    has_effect,

    -- ── Clasificadores calculados ─────────────────────────────────────────
    CASE WHEN card_type ILIKE '%Monster%' THEN TRUE ELSE FALSE END  AS is_monster,
    CASE WHEN card_type ILIKE '%Spell%'   THEN TRUE ELSE FALSE END  AS is_spell,
    CASE WHEN card_type ILIKE '%Trap%'    THEN TRUE ELSE FALSE END  AS is_trap,
    CASE WHEN card_type ILIKE ANY (
        '%Fusion%', '%Synchro%', '%XYZ%', '%Link%'
    )                                     THEN TRUE ELSE FALSE END  AS is_extra_deck

FROM {{ ref('bronze_flat_cards') }}