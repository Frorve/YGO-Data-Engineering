{{
    config(
        materialized = 'table',
        schema       = 'gold',
        alias        = 'fact_price_history',
        tags         = ['gold', 'fact']
    )
}}

/*
    Fact — fact_price_history
    ─────────────────────────
    Historial completo de precios de cada carta.
    Una fila por cada período de vigencia de un precio
    detectado por el snapshot SCD Type 2.
    Alimenta CU1 (evolución de precios) y CU4 (valoración de colección).

    Fuente: snap_card_prices (Silver)
    Joins:
      - dim_cards  → card_sk via card_id
      - dim_fecha  → fecha_sk via DATE(valid_from)
*/

WITH precios AS (

    SELECT
        scp.card_id,
        scp.cardmarket_price,
        scp.tcgplayer_price,
        scp.ebay_price,
        scp.amazon_price,
        scp.coolstuffinc_price,
        scp.best_sell_price,
        scp.price_margin_pct,
        scp.dbt_valid_from                          AS valid_from,
        scp.dbt_valid_to                            AS valid_to,
        IFF(scp.dbt_valid_to IS NULL, TRUE, FALSE)  AS is_current
    FROM {{ ref('snap_card_prices') }} scp

),

joined AS (

    SELECT
        p.*,
        dc.card_sk,
        df.fecha_sk
    FROM precios p
    LEFT JOIN {{ ref('dim_cards') }} dc
        ON p.card_id = dc.card_id
    LEFT JOIN {{ ref('dim_fecha') }} df
        ON TO_NUMBER(TO_CHAR(p.valid_from::DATE, 'YYYYMMDD')) = df.fecha_sk

)

SELECT
    -- ── Surrogate key ─────────────────────────────────────────────────────
    ROW_NUMBER() OVER (
        ORDER BY card_id, valid_from
    )                                               AS price_history_sk,

    -- ── Foreign keys ──────────────────────────────────────────────────────
    card_sk,
    fecha_sk,

    -- ── Identificación ────────────────────────────────────────────────────
    card_id,

    -- ── Precios ───────────────────────────────────────────────────────────
    cardmarket_price,
    tcgplayer_price,
    ebay_price,
    amazon_price,
    coolstuffinc_price,
    best_sell_price,
    price_margin_pct,

    -- ── Período de vigencia ───────────────────────────────────────────────
    valid_from,
    valid_to,
    is_current

FROM joined