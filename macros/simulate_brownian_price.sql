{% macro simulate_brownian_price(price_col, volatility, floor_pct, ceiling_pct) %}
/*
    Macro: simulate_brownian_price
    ──────────────────────────────
    Aplica un movimiento Browniano discreto sobre un precio base.
    El precio puede subir o bajar en cada ingesta con signo aleatorio.

    Parámetros:
        price_col   - Columna con el precio base
        volatility  - Volatilidad máxima por ingesta (0.03 = ±3%)
        floor_pct   - Límite mínimo como % del precio base (0.30 = 30%)
        ceiling_pct - Límite máximo como % del precio base (4.00 = 400%)
*/
CASE
    WHEN {{ price_col }} IS NULL OR {{ price_col }} = 0
    THEN {{ price_col }}
    ELSE
        GREATEST(
            {{ price_col }} * {{ floor_pct }},
            LEAST(
                {{ price_col }} * {{ ceiling_pct }},
                ROUND(
                    {{ price_col }} * (
                        1 + UNIFORM(
                            -{{ volatility }}::FLOAT,
                             {{ volatility }}::FLOAT,
                            RANDOM()
                        )
                    ),
                    2
                )
            )
        )
END
{% endmacro %}