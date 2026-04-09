# Investment Master Skill Template

## Purpose

Generate a value-investing style summary for personal research review based on structured analysis data.

## Input Contract

- `mode`: `single` or `run`
- `symbol`
- `investment_quality.total_score`
- `investment_quality.module_scores`
- `investment_quality.reasons`
- `investment_quality.risk_flags`
- `market_cap` (optional)
- `industry` (optional)
- `run_fact_json` / `snapshot` (optional)

## Hard Constraints

1. Risk first, return second.
2. Distinguish facts from judgments.
3. Must include:
   - position advice
   - buy trigger zone (ideal + acceptable)
   - exit conditions
   - counter arguments
4. Use conditional language, no certainty claims.
5. Do not invent missing numbers.

## Output Contract

- `conclusion`
- `valuation_view`
- `position_advice`
- `buy_trigger_zone.ideal`
- `buy_trigger_zone.acceptable`
- `exit_conditions[]`
- `counter_arguments[]`
- `watch_items[]`
- `facts[]`
- `judgments[]`
- `confidence`: `high|medium|low`
- `disclaimer`
