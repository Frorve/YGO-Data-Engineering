{% snapshot snap_card_banlists %}

{{
    config(
        target_database = var('env') | upper ~ '_SILVER_YGO_DB',
        target_schema   = 'silver',
        unique_key      = 'card_id',
        strategy        = 'check',
        check_cols      = [
            'ban_tcg',
            'ban_ocg',
            'ban_goat'
        ]
    )
}}

/*
    Snapshot — snap_card_banlists
    ─────────────────────────────
    Historial del estado de ban de cada carta mediante SCD Type 2.
    Detecta cambios en el ban list del TCG, OCG o GOAT entre ejecuciones.
    Alimenta el CU2 (IVA) y el CU3 (análisis de popularidad vs ban).

    Estrategia: check
      Compara los valores de ban_tcg, ban_ocg y ban_goat entre la fuente
      y el snapshot. Si alguno cambia cierra el registro anterior
      y abre uno nuevo.

    Campos automáticos de dbt:
      dbt_scd_id     → identificador único del registro histórico
      dbt_valid_from → momento en que este estado de ban entró en vigor
      dbt_valid_to   → momento en que dejó de ser válido (NULL = vigente)
      dbt_updated_at → timestamp de la última actualización
*/

SELECT
    card_id,
    ingesta_id,
    ingesta_ts,
    es_mock,
    ban_tcg,
    ban_ocg,
    ban_goat,
    is_banned_anywhere,
    ban_penalty_tcg
FROM {{ ref('stg_card_banlists') }}

{% endsnapshot %}