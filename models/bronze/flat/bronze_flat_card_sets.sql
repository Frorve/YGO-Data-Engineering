{{
    config(
        materialized    = 'incremental',
        schema          = 'bronze',
        alias           = 'flat_card_sets',
        unique_key      = ['ingesta_id', 'card_id', 'set_code', 'set_rarity'],
        on_schema_change = 'sync_all_columns',
        tags            = ['bronze', 'flat']
    )
}}

WITH raw AS (

    SELECT *
    FROM {{ ref('bronze_raw_cards') }}

    {% if is_incremental() %}
        WHERE ingesta_ts > (SELECT MAX(ingesta_ts) FROM {{ this }})
    {% endif %}

),

cartas AS (

    SELECT
        r.ingesta_id,
        r.entorno,
        r.ingesta_ts,
        r.es_mock,
        c.value AS carta_json
    FROM raw r,
    LATERAL FLATTEN(input => PARSE_JSON(r.raw_payload):data) c

),

sets_expandidos AS (

    SELECT
        cr.ingesta_id,
        cr.entorno,
        cr.ingesta_ts,
        cr.es_mock,
        cr.carta_json:id::INTEGER        AS card_id,
        cr.carta_json:name::VARCHAR      AS card_name,
        s.value:set_name::VARCHAR        AS set_name,
        s.value:set_code::VARCHAR        AS set_code,
        s.value:set_rarity::VARCHAR      AS set_rarity,
        s.value:set_rarity_code::VARCHAR AS set_rarity_code,
        s.value:set_price::FLOAT         AS set_price
    FROM cartas cr,
    LATERAL FLATTEN(input => cr.carta_json:card_sets) s

)

SELECT
    *,
    CURRENT_TIMESTAMP() AS flat_processed_ts
FROM sets_expandidos