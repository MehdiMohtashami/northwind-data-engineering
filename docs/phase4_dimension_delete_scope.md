# Phase 4 — Dimension delete via the stream (scoped proof-of-concept)

## What's implemented

CDC is enabled on `dbo.Suppliers` (capture instance `dbo_Suppliers`), with its
own row in `ETL_Settings.cdc_state` sharing the streaming watermark
(`stream_last_lsn`, advanced in lockstep with Orders/Order Details).

The producer polls it in the same window as Orders/Order Details and
publishes to `nw.cdc.suppliers`, keyed by `SupplierID`.

The consumer subscribes to that topic and, **only on a delete (`op == "D"`)**,
expires the current `DimSuppliers` row: `is_current = 0`, `Enddate = today`,
`Startdate` preserved, a new LSN-derived version. Inserts/updates on this
topic are intentionally ignored — those stay the batch SCD job's
responsibility (`load_dim_suppliers.py`), which already handles new/changed
attributes correctly. What batch SCD can't do is notice a business key that
*disappeared* from the source (it only ever compares against the latest
staging snapshot, so a vanished key just silently stops showing up — it's
never actively expired). This closes exactly that gap, for one dimension.

Tested: inserted a synthetic supplier (9001) into OLTP + a matching
`DimSuppliers` row directly, deleted the OLTP row, confirmed the dim row
flipped to `is_current=0` with `Enddate` set and `Startdate` preserved.

## What's NOT implemented (flagged, not built)

- The other four fact-referenced dims (`DimProducts`, `DimCustomer`,
  `DimEmployees`, `DimShippers`) don't have CDC enabled on their source
  tables and aren't wired into the producer/consumer. Same pattern would
  apply to each: enable CDC on the source table, add a `cdc_state` row, add
  a topic + producer poll + consumer delete-handler.
- `DimGeography` and `DimTerritories` have no single natural source table
  (geography is derived from a UNION across four tables; territories aren't
  fact-referenced), so this pattern doesn't map onto them directly.
- No monitoring on how many dimension rows are currently expired-by-stream
  vs by normal SCD2 history.

## A schema note this task surfaced

`DimSuppliers.version` and `FactOrders.version` were both `UInt32`;
LSN-derived versions (first 8 bytes of the CDC LSN, big-endian) need
`UInt64`. Both were widened. Any other dimension wired into the streaming
delete pattern later will need the same `version` column widening first.
