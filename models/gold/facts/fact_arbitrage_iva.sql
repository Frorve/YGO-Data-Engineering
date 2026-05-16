{{
    config(
        materialized = 'table',
        schema       = 'gold',
        alias        = 'fact_arbitrage_iva',
        tags         = ['gold', 'fact']
    )
}}

/*
    Fact — fact_arbitrage_iva
    ─────────────────────────
    Índice de Viabilidad de Arbitraje (IVA) calculado por carta.
    Una fila por carta sobre los precios y métricas vigentes hoy.
    Alimenta CU2 (análisis de oportunidades de arbitraje).

    Fórmula del IVA:
      IVA = Margen% × Factor_demanda × Penalización_precio × Penalización_ban

    Donde:
      Margen%             = price_margin_pct
      Factor_demanda      = viewsweek / MAX(viewsweek) normalizado 0-1
      Penalización_precio = 0.5 si cardmarket_price <= 0.10€, 1.0 si > 0.10€
      Penalización_ban    = macro ban_penalty (0.0 / 0.4 / 0.7 / 1.0)

    Semáforo:
      🟢 Alta  → IVA > 0.7
      🟡 Media → IVA entre 0.3 y 0.7
      🔴 Baja  → IVA < 0.3
*/

WITH precios_vigentes AS (

    SELECT
        card_id,
        cardmarket_price,
        tcgplayer_price,
        ebay_price,
        amazon_price,
        coolstuffinc_price,
        best_sell_price,
        price_margin_pct
    FROM {{ ref('snap_card_prices') }}
    WHERE dbt_valid_to IS NULL

),

banlists_vigentes AS (

    SELECT
        card_id,
        ban_tcg,
        ban_penalty_tcg
    FROM {{ ref('snap_card_banlists') }}
    WHERE dbt_valid_to IS NULL

),

popularidad AS (

    SELECT
        card_id,
        viewsweek
    FROM {{ ref('stg_cards') }}

),

max_viewsweek AS (

    SELECT MAX(viewsweek) AS max_views
    FROM {{ ref('stg_cards') }}
    WHERE viewsweek IS NOT NULL

),

calculo_iva AS (

    SELECT
        p.card_id,
        p.cardmarket_price,
        p.best_sell_price,
        p.price_margin_pct,

        -- ── Factor demanda normalizado 0-1 ────────────────────────────────
        CASE
            WHEN mv.max_views = 0 OR mv.max_views IS NULL THEN 0
            ELSE ROUND(
                COALESCE(pop.viewsweek, 0) / mv.max_views,
                4
            )
        END                                             AS factor_demanda,

        -- ── Penalización por precio bajo ──────────────────────────────────
        CASE
            WHEN p.cardmarket_price IS NULL
              OR p.cardmarket_price = 0   THEN 0.5
            WHEN p.cardmarket_price <= 0.10 THEN 0.5
            ELSE 1.0
        END                                             AS penalizacion_precio,

        -- ── Penalización por ban (macro centralizada) ─────────────────────
        {{ ban_penalty('b.ban_tcg') }}                  AS ban_penalty_tcg,

        b.ban_tcg,

        -- ── Plataforma óptima de venta ────────────────────────────────────
        CASE
            WHEN GREATEST(
                COALESCE(p.ebay_price, 0),
                COALESCE(p.tcgplayer_price, 0),
                COALESCE(p.amazon_price, 0)
            ) = COALESCE(p.ebay_price, 0)         THEN 'eBay'
            WHEN GREATEST(
                COALESCE(p.ebay_price, 0),
                COALESCE(p.tcgplayer_price, 0),
                COALESCE(p.amazon_price, 0)
            ) = COALESCE(p.tcgplayer_price, 0)    THEN 'TCGPlayer'
            ELSE 'Amazon'
        END                                             AS plataforma_optima

    FROM precios_vigentes p
    LEFT JOIN banlists_vigentes b
        ON p.card_id = b.card_id
    LEFT JOIN popularidad pop
        ON p.card_id = pop.card_id
    CROSS JOIN max_viewsweek mv

),

iva_calculado AS (

    SELECT
        *,
        -- ── IVA score final ───────────────────────────────────────────────
        ROUND(
            COALESCE(price_margin_pct, 0)
            * factor_demanda
            * penalizacion_precio
            * ban_penalty_tcg,
            4
        )                                               AS iva_score
    FROM calculo_iva

)

SELECT
    -- ── Surrogate key ─────────────────────────────────────────────────────
    ROW_NUMBER() OVER (
        ORDER BY iva_score DESC NULLS LAST
    )                                                   AS arbitrage_sk,

    -- ── Foreign keys ──────────────────────────────────────────────────────
    dc.card_sk,
    df.fecha_sk,

    -- ── Identificación ────────────────────────────────────────────────────
    ic.card_id,
    dc.card_name,

    -- ── Precios ───────────────────────────────────────────────────────────
    ic.cardmarket_price,
    ic.best_sell_price,
    ic.price_margin_pct,

    -- ── Componentes del IVA ───────────────────────────────────────────────
    ic.factor_demanda,
    ic.penalizacion_precio,
    ic.ban_penalty_tcg,
    ic.iva_score,

    -- ── Semáforo ──────────────────────────────────────────────────────────
    CASE
        WHEN ic.iva_score > 0.7  THEN 'Alta'
        WHEN ic.iva_score >= 0.3 THEN 'Media'
        ELSE 'Baja'
    END                                                 AS iva_semaforo,

    -- ── Plataforma óptima ─────────────────────────────────────────────────
    ic.plataforma_optima,

    -- ── Indicador vigencia ────────────────────────────────────────────────
    TRUE                                                AS is_current

FROM iva_calculado ic
LEFT JOIN {{ ref('dim_cards') }} dc
    ON ic.card_id = dc.card_id
LEFT JOIN {{ ref('dim_fecha') }} df
    ON df.fecha_date = CURRENT_DATE()