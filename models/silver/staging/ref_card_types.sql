{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'ref_card_types',
        tags         = ['silver', 'ref']
    )
}}

/*
    Referencia — ref_card_types
    ───────────────────────────
    Catálogo estático de todos los tipos de carta conocidos en YGOPRODeck.
    Se reconstruye completo en cada ejecución.
    No tiene incrementalidad.

    Categorías:
      Monster → cartas de monstruo de cualquier tipo
      Spell   → cartas mágicas
      Trap    → cartas trampa
      Token   → fichas de token
      Skill   → cartas de habilidad (formato Speed Duel)
*/

SELECT card_type_id, descripcion, categoria FROM (VALUES
    -- ── Monstruos Main Deck ───────────────────────────────────────────────
    ('Normal Monster',                    'Monstruo Normal sin efecto',                          'Monster'),
    ('Effect Monster',                    'Monstruo con efecto',                                 'Monster'),
    ('Flip Effect Monster',               'Monstruo con efecto de volteo',                       'Monster'),
    ('Flip Tuner Effect Monster',         'Monstruo Tuner con efecto de volteo',                 'Monster'),
    ('Gemini Monster',                    'Monstruo Gemini, necesita segunda invocación',        'Monster'),
    ('Normal Tuner Monster',              'Monstruo Tuner Normal sin efecto',                    'Monster'),
    ('Pendulum Effect Monster',           'Monstruo con efecto y zona Pendulum',                 'Monster'),
    ('Pendulum Effect Ritual Monster',    'Monstruo Ritual con efecto y zona Pendulum',          'Monster'),
    ('Pendulum Flip Effect Monster',      'Monstruo con efecto de volteo y zona Pendulum',       'Monster'),
    ('Pendulum Normal Monster',           'Monstruo Normal con zona Pendulum',                   'Monster'),
    ('Pendulum Tuner Effect Monster',     'Monstruo Tuner con efecto y zona Pendulum',           'Monster'),
    ('Ritual Effect Monster',             'Monstruo Ritual con efecto',                          'Monster'),
    ('Ritual Monster',                    'Monstruo Ritual Normal sin efecto',                   'Monster'),
    ('Spirit Monster',                    'Monstruo Spirit, regresa a la mano al final del turno','Monster'),
    ('Toon Monster',                      'Monstruo Toon, versión cartoon de cartas clásicas',   'Monster'),
    ('Tuner Monster',                     'Monstruo Tuner con efecto',                           'Monster'),
    ('Union Effect Monster',              'Monstruo Union, puede equiparse a otro monstruo',     'Monster'),
    -- ── Monstruos Extra Deck ─────────────────────────────────────────────
    ('Fusion Monster',                    'Monstruo de Fusión del Extra Deck',                   'Monster'),
    ('Link Monster',                      'Monstruo Link del Extra Deck sin DEF',                'Monster'),
    ('Pendulum Effect Fusion Monster',    'Monstruo de Fusión con zona Pendulum',                'Monster'),
    ('Synchro Monster',                   'Monstruo Sincronía del Extra Deck',                   'Monster'),
    ('Synchro Pendulum Effect Monster',   'Monstruo Sincronía con zona Pendulum',                'Monster'),
    ('Synchro Tuner Monster',             'Monstruo Sincronía que es Tuner',                     'Monster'),
    ('XYZ Monster',                       'Monstruo XYZ del Extra Deck, usa materiales',         'Monster'),
    ('XYZ Pendulum Effect Monster',       'Monstruo XYZ con zona Pendulum',                      'Monster'),
    -- ── Spell ────────────────────────────────────────────────────────────
    ('Spell Card',                        'Carta mágica de cualquier subtipo',                   'Spell'),
    -- ── Trap ─────────────────────────────────────────────────────────────
    ('Trap Card',                         'Carta trampa de cualquier subtipo',                   'Trap'),
    -- ── Token ────────────────────────────────────────────────────────────
    ('Token',                             'Ficha generada por el efecto de otra carta',          'Token'),
    -- ── Skill ────────────────────────────────────────────────────────────
    ('Skill Card',                        'Carta de habilidad exclusiva del formato Speed Duel', 'Skill')
) AS t(card_type_id, descripcion, categoria)