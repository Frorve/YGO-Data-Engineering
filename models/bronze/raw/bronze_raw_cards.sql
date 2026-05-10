{{
    config(
        materialized  = 'table',
        schema        = 'bronze',
        alias         = 'raw_cards',
        tags          = ['bronze', 'raw']
    )
}}

{% if var('env') == 'dev' %}

    SELECT
        ingesta_id,
        entorno,
        endpoint_url,
        http_status_code,
        raw_payload,
        ingesta_ts,
        es_mock
    FROM {{ ref('seed_raw_cards_dev') }}

{% else %}

    SELECT
        ingesta_id,
        entorno,
        endpoint_url,
        http_status_code,
        raw_payload,
        ingesta_ts,
        es_mock
    FROM {{ source('raw_ingesta', 'raw_api_ingestas') }}
    WHERE entorno = '{{ var("env") }}'

{% endif %}