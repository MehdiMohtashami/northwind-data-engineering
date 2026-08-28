# Phase 2 — ClickHouse vs professor DW column gap report

Verified via `SHOW CREATE TABLE` against every `NorthwindDW` table, compared
to `docs/professor_dw_schema.sql`. One fix applied (see below); everything
else is listed for a decision, not applied.

## Applied

- `FactOrders.Freight`: was `Decimal(19,4)` (non-nullable), professor has
  `MONEY NULL`. Changed to `Nullable(Decimal(19,4))` via `ALTER TABLE ...
  MODIFY COLUMN` and updated `infra/clickhouse/init/01_create_dw.sql`.

## Gaps found, NOT applied — for you to decide

| Table | Missing vs professor | Notes |
|---|---|---|
| `DimGeography` | `Address` | Professor keeps street address on the geography dim; we only have city/region/country/postal. |
| `DimSuppliers` | `Fax`, `HomePage` | We already carry `Address` here (professor doesn't — theirs lives on `DimGeography` instead). |
| `DimProducts` | `UnitsInStock`, `UnitsOnOrder`, `ReorderLevel` | Inventory-level attributes, not currently modeled. |
| `DimCustomer` | `Fax` | |
| `DimEmployees` | `HomePhone`, `Extension`, `Photo`, `Notes`, `PhotoPath`, raw `ReportsTo` | We keep only the resolved `ManagerKey`; professor keeps both `ParentEmployeeKey` (resolved) *and* raw `ReportsTo`. |
| `DimDate` | `CalendarSeason` / `SeasonName` | Our other date parts (`Day`, `Year`, `DayName`, etc.) are functionally equivalent to the professor's under different names — only the season grouping is genuinely missing. |
| `FactOrders` | `GeographyKey` (ship-to), raw `OrderDate`/`ShippedDate`/`RequiredDate` datetimes | Professor keeps the ship-to geography directly on the fact (independent of `CustomerKey`'s own geography) and denormalized raw dates alongside the surrogate `*DateKey` columns. |
| `FactEmployeeTerritories` | none | Exact match. |
| `DimShippers` | none | Exact match. |
| `DimTerritories` | none | Exact match. |

None of these block Phase 2's dimension/bridge loads. Flag which ones (if
any) you want backported before Phase 3 touches `FactOrders`.
