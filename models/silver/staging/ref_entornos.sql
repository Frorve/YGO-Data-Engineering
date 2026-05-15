{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'ref_entornos',
        tags         = ['silver', 'ref']
    )
}}

/*
    Referencia — ref_entornos
    ─────────────────────────
    Catálogo estático de los tres entornos del proyecto.
    Se reconstruye completo en cada ejecución.
    No tiene incrementalidad.
*/

SELECT 'dev' AS entorno_id, 'Entorno de desarrollo con datos mockeados'          AS descripcion
UNION ALL
SELECT 'pre' AS entorno_id, 'Entorno de preproducción con datos reales de la API' AS descripcion
UNION ALL
SELECT 'pro' AS entorno_id, 'Entorno de producción con datos reales de la API'    AS descripcion