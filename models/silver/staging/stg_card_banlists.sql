{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'stg_card_banlists',
        tags         = ['silver', 'staging']
    )
}}

/*
    Staging — stg_card_banlists
    ───────────────────────────
    Extrae y limpia el estado de ban de cada carta desde bronze_flat_cards.
    Separado de stg_cards porque el banlist cambia cada ~3 meses
    y alimenta directamente los snapshots de banlists y el IVA en Gold.
    Se reconstruye completa en cada ejecución.

    Campos calculados:
      is_banned_anywhere → TRUE si la carta es Forbidden en TCG u OCG
      ban_penalty_tcg    → factor de penalización para el IVA usando macro ban_penalty
*/

SELECT
    -- ── Metadatos de trazabilidad ─────────────────────────────────────────
    card_id,
    ingesta_id,
    ingesta_ts,
    es_mock,

    -- ── Estado de ban por formato ─────────────────────────────────────────
    ban_tcg,
    ban_ocg,
    ban_goat,

    -- ── Clasificador calculado ────────────────────────────────────────────
    CASE
        WHEN ban_tcg = 'Forbidden' OR ban_ocg = 'Forbidden'
        THEN TRUE
        ELSE FALSE
    END                                                 AS is_banned_anywhere,

    -- ── Factor de penalización del IVA (macro centralizada) ───────────────
    {{ ban_penalty('ban_tcg') }}                        AS ban_penalty_tcg

FROM {{ ref('bronze_flat_cards') }}