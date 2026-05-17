{{
    config(
        materialized    = 'incremental',
        schema          = 'bronze',
        alias           = 'flat_card_images',
        unique_key      = ['ingesta_id', 'card_id', 'image_id'],
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

imagenes_expandidas AS (

    SELECT
        cr.ingesta_id,
        cr.entorno,
        cr.ingesta_ts,
        cr.es_mock,
        cr.carta_json:id::INTEGER          AS card_id,
        cr.carta_json:name::VARCHAR        AS card_name,
        img.index::INTEGER                 AS artwork_index,
        img.value:id::INTEGER              AS image_id,
        img.value:image_url::VARCHAR       AS image_url,
        img.value:image_url_small::VARCHAR AS image_url_small,
        img.value:image_url_cropped::VARCHAR AS image_url_cropped
    FROM cartas cr,
    LATERAL FLATTEN(input => cr.carta_json:card_images) img

)

SELECT
    *,
    CURRENT_TIMESTAMP() AS flat_processed_ts
FROM imagenes_expandidas