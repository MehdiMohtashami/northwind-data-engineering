# Phase 1 — ClickHouse schema reconciliation notes

Source of truth: the professor's reference DW, restored in Phase 0 as SQL
Server database `Northwind_DW` (note: the backup's *logical file* names are
`Northwind_BI_1404_05_DW` / `_log` — that's a file name, not the database
name; see `docs/professor_dw_schema.sql` for the full reverse-engineered DDL).

## Changes applied to `infra/clickhouse/init/01_create_dw.sql`

1. **Removed `DimShipName`.** The professor's `FactOrders` has no ship-to
   dimension table — `ShipName` is a plain (degenerate) `NVARCHAR(80)` column
   directly on the fact. Our `FactOrders.ShipNameKey` was replaced with
   `ShipName Nullable(String)`.

2. **Added `DimTerritories`** (`ReplacingMergeTree(version)`, `ORDER BY
   TerritoryKey`): `TerritoryKey`, `TerritoryID` (natural key, professor's
   `TerritoryAlternateKey`), `RegionDescription`, `TerritoryDescription`,
   plus `is_current`/`Startdate`/`Enddate` since the professor's table carries
   SCD2 columns too.

3. **Added `FactEmployeeTerritories`** as a factless bridge
   (`ReplacingMergeTree(version)`, `ORDER BY (EmployeeKey, TerritoryKey)`,
   `is_deleted` + `version` for CDC-style merges). Grain: one row per
   employee/territory assignment, no measures — matches the professor's
   table exactly (its only columns are the two FK-like keys, no measure
   columns at all).

4. **Renamed `DimEmployee` → `DimEmployees`** to match the professor's naming.
   Kept `ManagerKey` (self-referencing surrogate) as our column name for the
   role the professor's `ParentEmployeeKey` plays — the professor's table
   *also* keeps the raw, unresolved `ReportsTo` natural key alongside
   `ParentEmployeeKey`; we did not add a redundant natural-key column since
   nothing downstream needs it yet.

5. **Updated `v_FactOrders_Flat`**: dropped the `DimShipName` join, selects
   `f.ShipName` directly from `FactOrders`; renamed the `DimEmployee` join to
   `DimEmployees`.

6. Database was **dropped and recreated** (dev-only, no real data loaded yet)
   against the new DDL. `DimDate` was repopulated (1990-01-01 → 2030-12-31,
   14,975 rows).

## `ETL_Settings.table_config` (SQL Server) kept in sync

- Removed `DimShipName`, `DimEmployee` rows.
- Added `DimEmployees` (scd_type=2), `DimTerritories` (scd_type=2),
  `FactEmployeeTerritories` (table_type=fact, scd_type=bridge, load_order=10,
  after `FactOrders`).
- `infra/sqlserver/init/01_etl_settings_and_cdc.sql` updated to match, so a
  from-scratch run seeds the corrected set.

## Deliberately NOT changed (out of scope for this reconciliation)

The professor's dims carry several extra columns we did not backport, since
Phase 1's ask was specifically about the four structural deltas above, not a
column-for-column match:
- `DimGeography.Address`, `DimSuppliers.Fax`/`HomePage`,
  `DimCustomer.Fax`, `DimShippers` has no SCD2 columns (professor treats it
  as static/SCD1, which already matches our design).
- `FactOrders` in the professor's DW also keeps a redundant `GeographyKey`
  (ship-to geography, independent of `CustomerKey`'s geography) and raw
  `OrderDate`/`ShippedDate`/`RequiredDate` datetime columns alongside the
  surrogate `*DateKey` columns. Our fact only has the surrogate date keys.

Flag these if a future phase wants tighter 1:1 parity with the professor's
design.
