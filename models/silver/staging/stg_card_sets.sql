{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'stg_card_sets',
        tags         = ['silver', 'staging']
    )
}}

/*
    Staging — stg_card_sets
    ───────────────────────
    Limpia y tipifica los registros de bronze_flat_card_sets.
    Una fila por cada combinación de carta, set y rareza.
    Usada en CU4 para valoración de colección y en CU2
    para ajustar liquidez por rareza.
    Se reconstruye completa en cada ejecución.

    Campos calculados:
      is_high_rarity → TRUE si la rareza es Secret Rare, Ultra Rare o superior
*/

SELECT
    -- ── Metadatos de trazabilidad ─────────────────────────────────────────
    card_id,
    ingesta_id,
    ingesta_ts,
    es_mock,

    -- ── Identificación ────────────────────────────────────────────────────
    TRIM(card_name)                                     AS card_name,

    -- ── Información del set ───────────────────────────────────────────────
    TRIM(set_name)                                      AS set_name,
    TRIM(set_code)                                      AS set_code,
    TRIM(set_rarity)                                    AS set_rarity,
    TRIM(set_rarity_code)                               AS set_rarity_code,
    set_price,

    -- ── Clasificador de rareza alta ───────────────────────────────────────
    CASE
        WHEN set_rarity ILIKE ANY (
            '%Secret Rare%',
            '%Ultra Rare%',
            '%Prismatic Secret Rare%',
            '%Quarter Century Secret Rare%',
            '%Starlight Rare%',
            '%Ghost Rare%',
            '%Collector%'
        ) THEN TRUE
        ELSE FALSE
    END                                                 AS is_high_rarity

FROM {{ ref('bronze_flat_card_sets') }}