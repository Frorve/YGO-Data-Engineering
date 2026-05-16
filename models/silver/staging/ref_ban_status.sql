{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'ref_ban_status',
        tags         = ['silver', 'ref']
    )
}}

/*
    Referencia — ref_ban_status
    ───────────────────────────
    Catálogo estático de los estados posibles en el ban list de Yu-Gi-Oh.
    Aplica a los formatos TCG, OCG y GOAT.
    Se reconstruye completo en cada ejecución.
    No tiene incrementalidad.
*/

SELECT ban_status_id, descripcion, copias_permitidas FROM (VALUES
    ('Forbidden',    'Carta prohibida. No se puede incluir ninguna copia en el mazo',         0),
    ('Limited',      'Carta limitada. Solo se puede incluir 1 copia en el mazo',              1),
    ('Semi-Limited', 'Carta semi-limitada. Se pueden incluir máximo 2 copias en el mazo',     2),
    ('Unlimited',    'Sin restricción. Se pueden incluir hasta 3 copias en el mazo',          3)
) AS t(ban_status_id, descripcion, copias_permitidas)