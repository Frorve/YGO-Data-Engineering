{{
    config(
        materialized    = 'incremental',
        schema          = 'bronze',
        alias           = 'flat_cards',
        unique_key      = ['ingesta_id', 'card_id'],
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
        c.value                                             AS carta_json

    FROM raw r,
    LATERAL FLATTEN(input => PARSE_JSON(r.raw_payload):data) c

),

campos_planos AS (

    SELECT
        -- Metadatos de ingesta
        ingesta_id,
        entorno,
        ingesta_ts,
        es_mock,

        -- Identificación
        carta_json:id::INTEGER                              AS card_id,
        carta_json:name::VARCHAR                            AS card_name,

        -- Tipo y clasificación
        carta_json:type::VARCHAR                            AS card_type,
        carta_json:frameType::VARCHAR                       AS frame_type,
        carta_json:humanReadableCardType::VARCHAR           AS human_readable_card_type,
        carta_json:desc::VARCHAR                            AS card_desc,

        -- Atributos de combate
        carta_json:race::VARCHAR                            AS race,
        carta_json:attribute::VARCHAR                       AS attribute,
        carta_json:atk::INTEGER                             AS atk,
        carta_json:def::INTEGER                             AS def,
        carta_json:level::INTEGER                           AS level,
        carta_json:archetype::VARCHAR                       AS archetype,

        -- Extras
        carta_json:linkval::INTEGER                         AS link_val,
        carta_json:scale::INTEGER                           AS pendulum_scale,
        carta_json:ygoprodeck_url::VARCHAR                  AS ygoprodeck_url,

        -- Precios originales de la API
        carta_json:card_prices[0]:cardmarket_price::FLOAT   AS cardmarket_price,
        carta_json:card_prices[0]:tcgplayer_price::FLOAT    AS tcgplayer_price,
        carta_json:card_prices[0]:ebay_price::FLOAT         AS ebay_price,
        carta_json:card_prices[0]:amazon_price::FLOAT       AS amazon_price,
        carta_json:card_prices[0]:coolstuffinc_price::FLOAT AS coolstuffinc_price,

        -- Banlist
        carta_json:banlist_info:ban_tcg::VARCHAR            AS ban_tcg,
        carta_json:banlist_info:ban_ocg::VARCHAR            AS ban_ocg,
        carta_json:banlist_info:ban_goat::VARCHAR           AS ban_goat,

        -- Misc info
        carta_json:misc_info[0]:views::INTEGER              AS views,
        carta_json:misc_info[0]:viewsweek::INTEGER          AS viewsweek,
        carta_json:misc_info[0]:upvotes::INTEGER            AS upvotes,
        carta_json:misc_info[0]:downvotes::INTEGER          AS downvotes,
        carta_json:misc_info[0]:tcg_date::DATE              AS tcg_date,
        carta_json:misc_info[0]:ocg_date::DATE              AS ocg_date,
        carta_json:misc_info[0]:konami_id::INTEGER          AS konami_id,
        carta_json:misc_info[0]:has_effect::INTEGER         AS has_effect,
        carta_json:misc_info[0]:md_rarity::VARCHAR          AS md_rarity,
        carta_json:misc_info[0]:treated_as::VARCHAR         AS treated_as

    FROM cartas

)

SELECT
    *,
    CURRENT_TIMESTAMP() AS flat_processed_ts
FROM campos_planos