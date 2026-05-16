{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'stg_card_images',
        tags         = ['silver', 'staging']
    )
}}

/*
    Staging — stg_card_images
    ─────────────────────────
    Limpia y tipifica los registros de bronze_flat_card_images.
    Una fila por cada imagen o artwork de cada carta.
    Usada en CU4 para mostrar la imagen de cada carta
    en la valoración de colección.
    Se reconstruye completa en cada ejecución.

    Campos calculados:
      is_main_artwork → TRUE si artwork_index = 0 (arte principal)
*/

SELECT
    -- ── Metadatos de trazabilidad ─────────────────────────────────────────
    card_id,
    ingesta_id,
    ingesta_ts,
    es_mock,

    -- ── Identificación ────────────────────────────────────────────────────
    TRIM(card_name)                                     AS card_name,

    -- ── Información de la imagen ──────────────────────────────────────────
    artwork_index,
    image_id,
    image_url,
    image_url_small,
    image_url_cropped,

    -- ── Clasificador de arte principal ────────────────────────────────────
    CASE
        WHEN artwork_index = 0 THEN TRUE
        ELSE FALSE
    END                                                 AS is_main_artwork

FROM {{ ref('bronze_flat_card_images') }}