# Phase 6A — Employee-photo data lake

## Design

Employee images live entirely outside the warehouse, in MinIO (S3-compatible
object storage), bucket `employee-photos`, keyed by **EmployeeCode**
(`EMP<EmployeeID>.png` — `EmployeeID` is the OLTP business key, i.e.
`EmployeeAlternateKey`). `DimEmployees` and MinIO are linked purely by that
key/URL — no foreign key, no binary data in ClickHouse. This is the
lakehouse pattern: the warehouse holds structured, queryable metadata; the
lake holds the actual unstructured objects.

Three layers:
1. **Object storage** — MinIO, bucket set to public-read (demo only; a real
   deployment would use signed URLs or a private bucket behind an app).
2. **Metadata catalog** — `NorthwindDW.employee_photo_catalog`
   (EmployeeCode, ObjectKey, Url, ContentType, SizeBytes, UploadedAt). This
   is the lake's own metadata layer, independent of the DW.
3. **DW link** — `DimEmployees.PhotoObjectKey` / `PhotoUrl`, populated
   deterministically from `EmployeeID` by `load_dim_employees.py` (Phase 2's
   SCD job) on every load, so the link survives SCD2 history and re-runs
   without needing to look up the catalog at load time.

## What's one-shot vs. what's ongoing

- `datalake/generate_avatars.py` and `datalake/link_photos_to_dw.py` are
  one-shot scripts (run once via a throwaway container on `nw_de_net`, not
  a standing compose service) — there's no ongoing "photo pipeline"; this
  is a bonus demo of the pattern, not a production photo-ingestion system.
- `load_dim_employees.py`'s `PhotoObjectKey`/`PhotoUrl` computation IS
  ongoing — every future dimension load (new employee, attribute change)
  gets the link set automatically, deterministically, with no dependency on
  the catalog table or MinIO being reachable at load time.

## A bug this surfaced, fixed project-wide

While writing `link_photos_to_dw.py` (using `clickhouse-connect`, which
validates integer ranges strictly), an epoch-millisecond version value
overflowed `DimEmployees.version`'s `UInt32` column. Checking the other
SCD2 dims showed the exact same problem, silently masked until now: the
Spark→JDBC write path truncates/wraps out-of-range integers instead of
erroring, so `DimGeography`, `DimProducts`, `DimCustomer`, `DimTerritories`,
`DimShippers`, and `FactEmployeeTerritories` were all storing wrapped
version values. They happened to still sort correctly by luck (all writes
so far occurred within one short testing window), but this wasn't
guaranteed. All were widened to `UInt64` (matching the fix already applied
to `FactOrders`/`DimSuppliers` in Phases 3-4). No data was lost — existing
values fit into `UInt64` unchanged; only future writes benefit from no
longer being able to overflow.
