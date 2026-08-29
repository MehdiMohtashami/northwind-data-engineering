# Phase 6C — Grafana redesign: 5 single-screen KPI dashboards

## What changed

`sales_overview.json` and `realtime_ops.json` (Phase 3-6A, cramped/scrolling,
truncated axis labels) are replaced by 5 purpose-built dashboards, each
designed to fit one 1920x1080 screen with no vertical scroll:
`nw_exec.json`, `nw_product.json`, `nw_customer.json`, `nw_sales_team.json`,
`nw_realtime.json`. `employee_directory.json` (Phase 6A data-lake showcase)
is untouched — it isn't one of the 5 KPI dashboards.

Every dimension with text labels (categories, products, customers,
countries, employees, shippers) uses a horizontal bar chart, Top-10-limited
where the dimension has many members, so names are never rotated or
truncated. Currency uses `currencyUSD` (which auto-abbreviates to K/M —
expected and fine for dollar amounts); raw counts use a custom formatter
(below) so KPI tiles show exact integers like `2,155`, never `2K`.

## Verification method

Grading a dashboard's layout by reading its JSON isn't reliable — grid units
don't reveal whether Grafana actually renders it without a scrollbar or with
a mislabeled axis. Playwright (installed into the session scratchpad, not a
project dependency) logged into Grafana, loaded each dashboard at exactly
1920x1080, and screenshotted the full page. `document.documentElement.
scrollHeight` vs `window.innerHeight` gave an objective pass/fail for
"no vertical scroll" per dashboard. This caught four real bugs no amount of
JSON review would have:

1. **A numeric-looking category axis silently misbehaves.** `Revenue by
   Year` grouped by `toString(OrderYear)` ("1996", "1997", "1998"). Because
   the strings were purely numeric, Grafana's barchart panel re-inferred the
   field as a number instead of a category, producing a bogus second data
   series and a nonsense axis label ("$2.00K", the currency-formatted value
   of ~1996-1998). Fixed by making the label genuinely non-numeric —
   `concat('CY ', toString(OrderYear))` — the same trick already used for
   `Q1`-`Q4` quarter labels, which never had the problem.

2. **Stat panels showing a string value rendered "No data".** Any stat
   panel whose query returns a non-numeric column (`Top Category`, and every
   count wrapped in `fmt_int`, see below) showed "No data" despite the query
   returning a valid row. Grafana's stat panel `reduceOptions.fields`
   defaults to `""` ("Numeric Fields" only) — a string-typed result gets
   filtered out before display. Fixed by setting `reduceOptions.fields:
   "/.*/"` ("All Fields") on every stat panel.

3. **A pie/donut chart legend showed field names instead of category
   values.** `Shipped vs Pending` (2 rows: `status`, `orders`) rendered one
   slice per *column* ("orders", "status") instead of one slice per *row*
   ("Shipped", "Pending") — because `reduceOptions.calcs: ["lastNotNull"]`
   reduces each field to one number, whereas a pie chart needs
   `reduceOptions.values: true` to treat each row as its own slice.

4. **`$__timeFrom()`/`$__timeTo()` aren't real macros in this plugin
   version** (`grafana-clickhouse-datasource`). They looked like the
   Postgres/MySQL-style zero-arg time macros but the ClickHouse plugin
   registers them as *tokens*, not functions — `$__timeFrom()` with parens
   throws `unexpected number of arguments`. The correct tokens (no
   parens) are `$__fromTime` / `$__toTime`. This broke `Rows Ingested per
   Minute`'s `WITH FILL` range, which showed a big red error icon and "No
   data" — exactly the eyesore the task explicitly asked to avoid.

## Precise integer formatting: `fmt_int`

Grafana's built-in units don't cover "exact integer, comma-grouped": `none`
shows no separators (`51317`), `short`/`currencyUSD` auto-abbreviate with
SI suffixes (`51.3 K`) regardless of the `decimals` setting. Since no unit
does both, the formatting was moved into ClickHouse itself — a
`CREATE FUNCTION fmt_int` (in `infra/clickhouse/init/01_create_dw.sql`,
so it's recreated automatically on a clean stack) that reverses the string,
chunks it into groups of 3, rejoins with commas, and reverses back:
`fmt_int(51317)` → `"51,317"`. Every stat-panel count (`Total Orders`,
`Distinct Employees`, `Current FactOrders Lines`, etc.) is wrapped in it.
Bar chart value labels keep plain `unit: "none"` (no comma support without
turning the bar's height field into a string, which would break the bar
chart itself) — acceptable since the hard requirement was specifically
about KPI tiles, not bar labels, and `none` still avoids the K/M
abbreviation that was the actual complaint.

## Layout

Each dashboard keeps total panel grid height to ≤21 units (24-wide grid):
a stat row (h=4), then two or three content rows (h=7-9 each). Confirmed
via the Playwright `scrollHeight` check on all 5, at 1996-01-01→1998-12-31
for the four analytical dashboards and now-6h/5s-refresh for Real-Time
Operations.
