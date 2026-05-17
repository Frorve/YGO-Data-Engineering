{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'ref_archetypes',
        tags         = ['silver', 'ref']
    )
}}

/*
    Referencia — ref_archetypes
    ───────────────────────────
    Catálogo de arquetipos extraídos directamente de bronze_flat_cards.
    A diferencia del resto de tablas ref_, esta no tiene valores predefinidos
    sino que se construye dinámicamente desde los datos ingestados.
    Konami añade nuevos arquetipos constantemente.
    Se reconstruye completo en cada ejecución.
    No tiene incrementalidad.
*/

SELECT DISTINCT
    archetype                AS archetype_id,
    archetype                AS descripcion
FROM {{ ref('bronze_flat_cards') }}
WHERE archetype IS NOT NULL
ORDER BY archetype