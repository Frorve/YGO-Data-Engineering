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
        c.value AS carta_json
    FROM raw r,
    LATERAL FLATTEN(input => PARSE_JSON(r.raw_payload):data) c

),

campos_planos AS (

    SELECT
        ingesta_id,
        entorno,
        ingesta_ts,
        es_mock,
        carta_json:id::INTEGER                              AS card_id,
        carta_json:name::VARCHAR                            AS card_name,
        carta_json:type::VARCHAR                            AS card_type,
        carta_json:frameType::VARCHAR                       AS frame_type,
        carta_json:humanReadableCardType::VARCHAR           AS human_readable_card_type,
        carta_json:desc::VARCHAR                            AS card_desc,
        carta_json:race::VARCHAR                            AS race,
        carta_json:attribute::VARCHAR                       AS attribute,
        carta_json:atk::INTEGER                             AS atk,
        carta_json:def::INTEGER                             AS def,
        carta_json:level::INTEGER                           AS level,
        carta_json:archetype::VARCHAR                       AS archetype,
        carta_json:linkval::INTEGER                         AS link_val,
        carta_json:scale::INTEGER                           AS pendulum_scale,
        carta_json:ygoprodeck_url::VARCHAR                  AS ygoprodeck_url,

        -- Precios originales de la API (siempre se conservan)
        carta_json:card_prices[0]:cardmarket_price::FLOAT   AS api_cardmarket_price,
        carta_json:card_prices[0]:tcgplayer_price::FLOAT    AS api_tcgplayer_price,
        carta_json:card_prices[0]:ebay_price::FLOAT         AS api_ebay_price,
        carta_json:card_prices[0]:amazon_price::FLOAT       AS api_amazon_price,
        carta_json:card_prices[0]:coolstuffinc_price::FLOAT AS api_coolstuffinc_price,

        -- Banlist
        carta_json:banlist_info:ban_tcg::VARCHAR            AS ban_tcg,
        carta_json:banlist_info:ban_ocg::VARCHAR            AS ban_ocg,
        carta_json:banlist_info:ban_goat::VARCHAR           AS ban_goat,

        -- Misc info
        carta_json:misc_info[0]:views::INTEGER              AS api_views,
        carta_json:misc_info[0]:viewsweek::INTEGER          AS api_viewsweek,
        carta_json:misc_info[0]:upvotes::INTEGER            AS upvotes,
        carta_json:misc_info[0]:downvotes::INTEGER          AS downvotes,
        carta_json:misc_info[0]:tcg_date::DATE              AS tcg_date,
        carta_json:misc_info[0]:ocg_date::DATE              AS ocg_date,
        carta_json:misc_info[0]:konami_id::INTEGER          AS konami_id,
        carta_json:misc_info[0]:has_effect::INTEGER         AS has_effect,
        carta_json:misc_info[0]:md_rarity::VARCHAR          AS md_rarity,
        carta_json:misc_info[0]:treated_as::VARCHAR         AS treated_as

    FROM cartas

),

precios_simulados AS (

    SELECT
        cp.*,

        {% if var('enable_brownian_mock') %}

        -- Precios simulados con Brownian Motion
        {{ simulate_brownian_price(
            'api_cardmarket_price',
            var('brownian_volatility_cardmarket'),
            var('brownian_floor_pct'),
            var('brownian_ceiling_pct')
        ) }} AS cardmarket_price,

        {{ simulate_brownian_price(
            'api_tcgplayer_price',
            var('brownian_volatility_tcgplayer'),
            var('brownian_floor_pct'),
            var('brownian_ceiling_pct')
        ) }} AS tcgplayer_price,

        {{ simulate_brownian_price(
            'api_ebay_price',
            var('brownian_volatility_ebay'),
            var('brownian_floor_pct'),
            var('brownian_ceiling_pct')
        ) }} AS ebay_price,

        {{ simulate_brownian_price(
            'api_amazon_price',
            var('brownian_volatility_amazon'),
            var('brownian_floor_pct'),
            var('brownian_ceiling_pct')
        ) }} AS amazon_price,

        {{ simulate_brownian_price(
            'api_coolstuffinc_price',
            var('brownian_volatility_coolstuff'),
            var('brownian_floor_pct'),
            var('brownian_ceiling_pct')
        ) }} AS coolstuffinc_price,

        -- Views simuladas
        GREATEST(0,
            api_views + FLOOR(
                UNIFORM(
                    {{ var('brownian_views_min_delta') }}::FLOAT,
                    {{ var('brownian_views_max_delta') }}::FLOAT,
                    RANDOM()
                )
            )
        )::INTEGER AS views,

        GREATEST(0,
            ROUND(
                api_viewsweek * (
                    1 + UNIFORM(
                        {{ var('brownian_viewsweek_min') }}::FLOAT,
                        {{ var('brownian_viewsweek_max') }}::FLOAT,
                        RANDOM()
                    )
                )
            )
        )::INTEGER AS viewsweek,

        TRUE AS is_mocked

        {% else %}

        -- Precios reales sin modificar
        api_cardmarket_price    AS cardmarket_price,
        api_tcgplayer_price     AS tcgplayer_price,
        api_ebay_price          AS ebay_price,
        api_amazon_price        AS amazon_price,
        api_coolstuffinc_price  AS coolstuffinc_price,
        api_views               AS views,
        api_viewsweek           AS viewsweek,
        FALSE                   AS is_mocked

        {% endif %}

    FROM campos_planos cp

)

SELECT
    ingesta_id,
    entorno,
    ingesta_ts,
    es_mock,
    is_mocked,
    card_id,
    card_name,
    card_type,
    frame_type,
    human_readable_card_type,
    card_desc,
    race,
    attribute,
    atk,
    def,
    level,
    archetype,
    link_val,
    pendulum_scale,
    ygoprodeck_url,
    api_cardmarket_price,
    api_tcgplayer_price,
    api_ebay_price,
    api_amazon_price,
    api_coolstuffinc_price,
    cardmarket_price,
    tcgplayer_price,
    ebay_price,
    amazon_price,
    coolstuffinc_price,
    ban_tcg,
    ban_ocg,
    ban_goat,
    api_views,
    api_viewsweek,
    views,
    viewsweek,
    upvotes,
    downvotes,
    tcg_date,
    ocg_date,
    konami_id,
    has_effect,
    md_rarity,
    treated_as,
    CURRENT_TIMESTAMP() AS flat_processed_ts

FROM precios_simulados