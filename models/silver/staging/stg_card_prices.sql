{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'stg_card_prices',
        tags         = ['silver', 'staging']
    )
}}

/*
    Staging — stg_card_prices
    ─────────────────────────
    Extrae y limpia los precios de cada carta desde bronze_flat_cards.
    Separado de stg_cards porque los precios cambian con el tiempo
    y alimentan directamente los snapshots y el cálculo del IVA en Gold.
    Se reconstruye completa en cada ejecución.

    Precios:
      cardmarket_price → precio de compra en Europa (€)
      tcgplayer_price  → precio de venta potencial ($)
      ebay_price       → precio de venta potencial ($)
      amazon_price     → precio de venta potencial ($)
      coolstuffinc_price → precio de venta potencial ($)

    Campos calculados:
      best_sell_price  → MAX(ebay, tcgplayer, amazon)
      price_margin_pct → (best_sell_price - cardmarket_price) / cardmarket_price
*/

SELECT
    -- ── Metadatos de trazabilidad ─────────────────────────────────────────
    card_id,
    ingesta_id,
    ingesta_ts,
    es_mock,

    -- ── Precios originales ────────────────────────────────────────────────
    cardmarket_price,
    tcgplayer_price,
    ebay_price,
    amazon_price,
    coolstuffinc_price,

    -- ── Mejor precio de venta entre plataformas ───────────────────────────
    GREATEST(
        COALESCE(ebay_price, 0),
        COALESCE(tcgplayer_price, 0),
        COALESCE(amazon_price, 0)
    )                                                   AS best_sell_price,

    -- ── Margen de arbitraje ───────────────────────────────────────────────
    -- Null si cardmarket_price es null o 0 para evitar división por cero
    CASE
        WHEN cardmarket_price IS NULL OR cardmarket_price = 0 THEN NULL
        ELSE ROUND(
            (
                GREATEST(
                    COALESCE(ebay_price, 0),
                    COALESCE(tcgplayer_price, 0),
                    COALESCE(amazon_price, 0)
                ) - cardmarket_price
            ) / cardmarket_price,
            4
        )
    END                                                 AS price_margin_pct

FROM {{ ref('bronze_flat_cards') }}