{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'ref_attributes',
        tags         = ['silver', 'ref']
    )
}}

/*
    Referencia — ref_attributes
    ───────────────────────────
    Catálogo estático de los 7 atributos de monstruo en Yu-Gi-Oh.
    Solo aplica a cartas de tipo monstruo.
    Se reconstruye completo en cada ejecución.
    No tiene incrementalidad.
*/

SELECT attribute_id, descripcion FROM (VALUES
    ('DARK',   'Atributo Oscuridad. Asociado a demonios, no muertos y criaturas de la sombra'),
    ('EARTH',  'Atributo Tierra. Asociado a bestias, rocas y criaturas terrestres'),
    ('FIRE',   'Atributo Fuego. Asociado a dragones de fuego y criaturas piromaníacas'),
    ('LIGHT',  'Atributo Luz. Asociado a ángeles, máquinas y seres celestiales'),
    ('WATER',  'Atributo Agua. Asociado a criaturas acuáticas y serpientes marinas'),
    ('WIND',   'Atributo Viento. Asociado a aves aladas e insectos'),
    ('DIVINE', 'Atributo Divino. Exclusivo de los Dioses Egipcios y criaturas supremas')
) AS t(attribute_id, descripcion)