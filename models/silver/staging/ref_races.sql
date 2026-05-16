{{
    config(
        materialized = 'table',
        schema       = 'silver',
        alias        = 'ref_races',
        tags         = ['silver', 'ref']
    )
}}

/*
    Referencia — ref_races
    ──────────────────────
    Catálogo estático de razas de monstruos y subtipos de Spell y Trap.
    Se reconstruye completo en cada ejecución.
    No tiene incrementalidad.
*/

SELECT race_id, descripcion, aplica_a FROM (VALUES
    -- ── Razas de monstruo ─────────────────────────────────────────────────
    ('Aqua',            'Criaturas acuáticas como peces y seres del agua',          'Monster'),
    ('Beast',           'Bestias y animales salvajes',                              'Monster'),
    ('Beast-Warrior',   'Bestias con características guerreras',                    'Monster'),
    ('Creator-God',     'Dioses creadores, los más poderosos del juego',            'Monster'),
    ('Cyberse',         'Criaturas digitales del mundo virtual',                    'Monster'),
    ('Dinosaur',        'Dinosaurios y criaturas prehistóricas',                    'Monster'),
    ('Divine-Beast',    'Bestias divinas, incluye los Dioses Egipcios',             'Monster'),
    ('Dragon',          'Dragones de todo tipo',                                    'Monster'),
    ('Fairy',           'Seres celestiales y angélicos',                            'Monster'),
    ('Fiend',           'Demonios y criaturas del inframundo',                      'Monster'),
    ('Fish',            'Peces y criaturas marinas menores',                        'Monster'),
    ('Insect',          'Insectos y artrópodos',                                    'Monster'),
    ('Machine',         'Máquinas y robots',                                        'Monster'),
    ('Plant',           'Plantas y criaturas vegetales',                            'Monster'),
    ('Psychic',         'Seres con poderes mentales y psíquicos',                   'Monster'),
    ('Pyro',            'Criaturas de fuego y llamas',                              'Monster'),
    ('Reptile',         'Reptiles y serpientes',                                    'Monster'),
    ('Rock',            'Criaturas de piedra y minerales',                          'Monster'),
    ('Sea Serpent',     'Serpientes marinas y criaturas de las profundidades',      'Monster'),
    ('Spellcaster',     'Magos y usuarios de magia',                                'Monster'),
    ('Thunder',         'Criaturas eléctricas y del trueno',                        'Monster'),
    ('Warrior',         'Guerreros y soldados humanoides',                          'Monster'),
    ('Winged Beast',    'Criaturas voladoras con alas',                             'Monster'),
    ('Wyrm',            'Criaturas similares a dragones pero distintas',            'Monster'),
    ('Zombie',          'Muertos vivientes y criaturas no muertas',                 'Monster'),
    -- ── Subtipos de Spell ─────────────────────────────────────────────────
    ('Normal',          'Spell o Trap Normal, se activa y va al cementerio',        'Spell'),
    ('Field',           'Spell de Campo, modifica las condiciones del terreno',     'Spell'),
    ('Equip',           'Spell de Equipo, se equipa a un monstruo',                 'Spell'),
    ('Continuous',      'Spell Continua, permanece en el campo activa',             'Spell'),
    ('Quick-Play',      'Spell de Juego Rápido, activable en turno del rival',      'Spell'),
    ('Ritual',          'Spell Ritual, necesaria para invocar monstruos Ritual',    'Spell'),
    -- ── Subtipos de Trap ──────────────────────────────────────────────────
    ('Counter',         'Trap Counter, responde a activaciones de cartas',          'Trap')
) AS t(race_id, descripcion, aplica_a)