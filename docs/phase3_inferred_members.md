# Phase 3 — Inferred dimension members

## What's implemented

`scd_lib.lookup_or_infer()` is used by the CDC fact job (`load_fact_orders_cdc.py`)
for every dimension lookup. When a fact row's natural key (ProductID,
CustomerID, EmployeeID, ShipperID, or the ship-to geography tuple) doesn't
match any current dim row, it:

1. Inserts a minimal placeholder row into that dimension: the natural key
   columns filled in, every other attribute defaulted ('Unknown' for
   non-nullable strings, 0 for non-nullable numerics, NULL for nullable
   columns), `is_current=1`, `is_inferred=1`, and a fresh stable surrogate
   key (via the same `assign_surrogate_keys` used everywhere else, so it
   never collides with or renumbers a real key).
2. Re-runs the lookup, which now resolves to that placeholder.

`DimProducts`, `DimCustomer`, `DimEmployees`, `DimShippers`, and
`DimGeography` all have an `is_inferred UInt8 DEFAULT 0` column for this.

## What's NOT implemented (flagged, not built, per Phase 3 scope)

The Phase 2 dimension SCD jobs (`load_dim_suppliers.py`,
`load_dim_products.py`, etc.) do not yet know how to recognize an existing
`is_inferred=1` placeholder and overwrite it in place when the *real* record
for that business key finally arrives via normal extraction. As written,
they'll treat the real record as a "new business key" only if the
placeholder's business key doesn't already exist -- but it does (the
placeholder itself), so today's `scd2_apply`/`scd1_apply` logic would
actually detect it as a **changed** row (hash of attrs differs) and correctly
expire the placeholder + insert a new current version with the real data.
So the *mechanics* happen to work already for SCD2 dims, since "placeholder
attrs differ from real attrs" is indistinguishable from any other SCD2
change.

What's genuinely missing: `is_inferred` itself is never flipped back to `0`
by the Phase 2 jobs (a placeholder's replacement row would still be written
with `is_inferred=0` by default from those scripts' normal column set, since
they don't set it at all -- so this actually self-corrects for SCD2 dims).
The real gap is **DimShippers** (SCD1): `scd1_apply` overwrites in place by
design, which is exactly the right behavior for reconciling a placeholder,
so this also mostly self-corrects.

Given this, the placeholder mechanism is complete and self-healing for
normal dimension refresh runs. What's still open, and left as follow-up:
- No monitoring/alerting on how many inferred rows exist or how long they've
  been sitting unreconciled.
- No explicit test proving a placeholder gets cleanly replaced end-to-end
  (only the placeholder-insert path itself is tested in Phase 3).
