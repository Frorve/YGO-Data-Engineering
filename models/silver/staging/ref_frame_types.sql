{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'ref_frame_types',
        tags         = ['silver', 'ref']
    )
}}

/*
    Referencia — ref_frame_types
    ────────────────────────────
    Catálogo estático de todos los tipos de marco visual de las cartas.
    El frame_type determina el color y diseño del fondo de la carta.
    Se reconstruye completo en cada ejecución.
    No tiene incrementalidad.
*/

SELECT frame_type_id, descripcion FROM (VALUES
    -- ── Monstruos Main Deck ───────────────────────────────────────────────
    ('normal',              'Marco amarillo claro. Monstruo Normal sin efecto'),
    ('effect',              'Marco naranja. Monstruo con efecto'),
    ('ritual',              'Marco azul claro. Monstruo Ritual'),
    ('fusion',              'Marco morado. Monstruo de Fusión'),
    ('synchro',             'Marco blanco. Monstruo Sincronía'),
    ('xyz',                 'Marco negro. Monstruo XYZ'),
    ('link',                'Marco azul oscuro con red. Monstruo Link'),
    -- ── Pendulum ─────────────────────────────────────────────────────────
    ('normal_pendulum',     'Marco amarillo con degradado verde. Monstruo Normal Pendulum'),
    ('effect_pendulum',     'Marco naranja con degradado verde. Monstruo Efecto Pendulum'),
    ('ritual_pendulum',     'Marco azul claro con degradado verde. Monstruo Ritual Pendulum'),
    ('fusion_pendulum',     'Marco morado con degradado verde. Monstruo Fusión Pendulum'),
    ('synchro_pendulum',    'Marco blanco con degradado verde. Monstruo Sincronía Pendulum'),
    ('xyz_pendulum',        'Marco negro con degradado verde. Monstruo XYZ Pendulum'),
    -- ── Spell y Trap ─────────────────────────────────────────────────────
    ('spell',               'Marco verde. Carta mágica'),
    ('trap',                'Marco rosa. Carta trampa'),
    -- ── Otros ────────────────────────────────────────────────────────────
    ('token',               'Marco gris. Ficha de token'),
    ('skill',               'Marco azul degradado. Carta de habilidad Speed Duel')
) AS t(frame_type_id, descripcion)