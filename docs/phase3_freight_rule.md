# Phase 3 — Freight storage rule

Checked the professor's reference `Northwind_DW.dbo.FactOrders` at line grain:

```
OrderID  ProductKey  Freight  UnitPrice  Quantity
10248    11          32.38    14.0000    12
10248    42          32.38    9.8000     10
10248    72          32.38    34.8000    5
```

`Freight` is an **order-level** value, repeated identically on every line of
that order (not split, not zeroed on all-but-one line). Our `FactOrders` load
matches this: every detail line of an order carries the same `Freight` value
from `stg_orders`.

**Caveat for downstream dashboards:** `SUM(Freight)` over `FactOrders` at line
grain double/triple/N-counts freight once per order line. Any freight
aggregation must first collapse to order grain, e.g.
`SUM(Freight) OVER (PARTITION BY OrderID)` then dedupe, or
`sum(Freight) / count()` per order group, or simply
`SELECT sum(Freight) FROM (SELECT DISTINCT OrderID, Freight FROM FactOrders)`.
Flag this explicitly when building the Phase 4+ Grafana panels.
