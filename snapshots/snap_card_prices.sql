{% snapshot snap_card_prices %}

{{
    config(
        target_database = var('env') | upper ~ '_SILVER_YGO_DB',
        target_schema   = 'silver',
        unique_key      = 'card_id',
        strategy        = 'check',
        check_cols      = [
            'cardmarket_price',
            'tcgplayer_price',
            'ebay_price',
            'amazon_price',
            'coolstuffinc_price'
        ]
    )
}}

/*
    Snapshot — snap_card_prices
    ───────────────────────────
    Historial de precios de cada carta mediante SCD Type 2.
    Detecta cambios en cualquiera de los 5 precios entre ejecuciones.
    Alimenta el CU1 (evolución de precios) y el CU2 (IVA).

    Estrategia: check
      Compara los valores de check_cols entre la fuente y el snapshot.
      Si alguno cambia cierra el registro anterior y abre uno nuevo.

    Campos automáticos de dbt:
      dbt_scd_id     → identificador único del registro histórico
      dbt_valid_from → momento en que este precio entró en vigor
      dbt_valid_to   → momento en que este precio dejó de ser válido (NULL = vigente)
      dbt_updated_at → timestamp de la última actualización
*/

SELECT
    card_id,
    ingesta_id,
    ingesta_ts,
    es_mock,
    cardmarket_price,
    tcgplayer_price,
    ebay_price,
    amazon_price,
    coolstuffinc_price,
    best_sell_price,
    price_margin_pct
FROM {{ ref('stg_card_prices') }}

{% endsnapshot %}