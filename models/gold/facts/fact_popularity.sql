{{
    config(
        materialized = 'table',
        schema       = 'gold',
        alias        = 'fact_popularity',
        tags         = ['gold', 'fact']
    )
}}

/*
    Fact — fact_popularity
    ──────────────────────
    Métricas de popularidad y demanda de cada carta.
    Una fila por carta con las métricas vigentes del último snapshot.
    Alimenta CU3 (análisis de popularidad y demanda).

    Fuente: stg_cards + stg_card_banlists (Silver)
    Joins:
      - dim_cards      → card_sk via card_id
      - dim_archetypes → archetype_sk via archetype_id
      - dim_fecha      → fecha_sk via CURRENT_DATE()

    Campos calculados:
      - ratio_positivo: upvotes / (upvotes + downvotes)
      - tendencia_views: SUBIENDO si viewsweek > media global,
                         BAJANDO si viewsweek < media global * 0.5,
                         ESTABLE en caso contrario
*/

WITH cartas AS (

    SELECT
        card_id,
        archetype_id,
        views,
        viewsweek,
        upvotes,
        downvotes
    FROM {{ ref('stg_cards') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY card_id
        ORDER BY ingesta_ts DESC
    ) = 1

),

banlists AS (

    SELECT
        card_id,
        ban_tcg,
        is_banned_anywhere,
        ban_penalty_tcg
    FROM {{ ref('snap_card_banlists') }}
    WHERE dbt_valid_to IS NULL

),

media_global AS (

    SELECT AVG(viewsweek) AS avg_viewsweek
    FROM {{ ref('stg_cards') }}
    WHERE viewsweek IS NOT NULL

),

metricas AS (

    SELECT
        c.card_id,
        c.archetype_id,
        c.views,
        c.viewsweek,
        c.upvotes,
        c.downvotes,

        -- ── Ratio positivo ────────────────────────────────────────────────
        CASE
            WHEN COALESCE(c.upvotes, 0) + COALESCE(c.downvotes, 0) = 0
            THEN NULL
            ELSE ROUND(
                c.upvotes / (c.upvotes + c.downvotes),
                4
            )
        END                                             AS ratio_positivo,

        -- ── Tendencia de views ────────────────────────────────────────────
        CASE
            WHEN c.viewsweek > mg.avg_viewsweek        THEN 'SUBIENDO'
            WHEN c.viewsweek < mg.avg_viewsweek * 0.5  THEN 'BAJANDO'
            ELSE 'ESTABLE'
        END                                             AS tendencia_views,

        b.ban_tcg,
        b.is_banned_anywhere,
        b.ban_penalty_tcg

    FROM cartas c
    LEFT JOIN banlists b
        ON c.card_id = b.card_id
    CROSS JOIN media_global mg

)

SELECT
    -- ── Surrogate key ─────────────────────────────────────────────────────
    ROW_NUMBER() OVER (
        ORDER BY m.viewsweek DESC NULLS LAST
    )                                                   AS popularity_sk,

    -- ── Foreign keys ──────────────────────────────────────────────────────
    dc.card_sk,
    da.archetype_sk,
    df.fecha_sk,

    -- ── Identificación ────────────────────────────────────────────────────
    m.card_id,
    dc.card_name,

    -- ── Métricas de popularidad ───────────────────────────────────────────
    m.views,
    m.viewsweek,
    m.upvotes,
    m.downvotes,
    m.ratio_positivo,
    m.tendencia_views,

    -- ── Contexto de ban ───────────────────────────────────────────────────
    m.ban_tcg,
    COALESCE(m.is_banned_anywhere, FALSE)               AS is_banned_anywhere,
    COALESCE(m.ban_penalty_tcg, 1.0)                    AS ban_penalty_tcg,

    -- ── Indicador vigencia ────────────────────────────────────────────────
    TRUE                                                AS is_current

FROM metricas m
LEFT JOIN {{ ref('dim_cards') }} dc
    ON m.card_id = dc.card_id
LEFT JOIN {{ ref('dim_archetypes') }} da
    ON m.archetype_id = da.archetype_name
LEFT JOIN {{ ref('dim_fecha') }} df
    ON df.fecha_date = CURRENT_DATE()