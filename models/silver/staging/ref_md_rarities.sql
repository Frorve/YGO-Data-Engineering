{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'ref_md_rarities',
        tags         = ['silver', 'ref']
    )
}}

/*
    Referencia — ref_md_rarities
    ────────────────────────────
    Catálogo estático de rarezas del videojuego Master Duel.
    Independiente de las rarezas del juego de cartas físico.
    Se reconstruye completo en cada ejecución.
    No tiene incrementalidad.
*/

SELECT md_rarity_id, descripcion, orden FROM (VALUES
    ('Secret Rare', 'Rareza más alta en Master Duel. Marco dorado con efecto prisma', 1),
    ('Ultra Rare',  'Segunda rareza más alta. Marco dorado',                           2),
    ('Super Rare',  'Tercera rareza. Ilustración con efecto holográfico',              3),
    ('Rare',        'Rareza estándar. Nombre de la carta en plateado',                 4),
    ('Normal',      'Rareza base. Sin efectos especiales en el arte',                  5)
) AS t(md_rarity_id, descripcion, orden)