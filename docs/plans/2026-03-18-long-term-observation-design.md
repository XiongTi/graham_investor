# Long-Term Observation Design

## Goal

Track two fixed US stock portfolios from 2026-03-18 onward with:

- per-stock daily return
- per-stock cumulative return
- portfolio daily return
- portfolio cumulative return

The tool should print a readable terminal report and append/update CSV logs on each run.

## Scope

- Fixed groups: `fallback_top10` and `market_top10`
- Fixed inception date: `2026-03-18`
- Capital: `30000 USD` per portfolio
- Equal-weight allocation
- No dividends, taxes, slippage, or FX handling

## Output

- `data/daily_positions.csv`
- `data/daily_portfolios.csv`
- terminal summary on each run

## Data Handling

- Buy price: first available close on or after inception date
- Daily return: latest close vs previous trading close
- Total return: latest close vs inception buy price
- If a day is not a trading day, use the latest available close and keep the actual price date

## Rationale

CSV append/update is simpler than introducing a database, and long-table CSV format is easier to analyze later with pandas or spreadsheets.
