{% macro ban_penalty(ban_col) %}
/*
    Macro: ban_penalty
    ──────────────────
    Devuelve el factor de penalización del IVA según el estado
    de la carta en el banlist. Centralizada aquí para que cualquier
    cambio en las reglas de Konami se propague automáticamente
    a todos los modelos Gold que usen esta macro.

    Forbidden    → 0.0  carta ilegal, IVA = 0
    Limited      → 0.4  riesgo alto, solo 1 copia permitida
    Semi-Limited → 0.7  riesgo moderado, máximo 2 copias
    Unlimited    → 1.0  sin restricción
*/
CASE
    WHEN UPPER({{ ban_col }}) = 'FORBIDDEN'    THEN 0.0
    WHEN UPPER({{ ban_col }}) = 'LIMITED'      THEN 0.4
    WHEN UPPER({{ ban_col }}) = 'SEMI-LIMITED' THEN 0.7
    ELSE 1.0
END
{% endmacro %}