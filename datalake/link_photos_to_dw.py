"""
One-shot: loads the data lake's photo metadata (datalake/photo_catalog.json,
produced by generate_avatars.py) into NorthwindDW.employee_photo_catalog, and
backfills PhotoObjectKey/PhotoUrl onto every CURRENT DimEmployees row.

Follows this project's established ReplacingMergeTree pattern throughout:
DimEmployees rows are never UPDATEd in place -- a new version of each row is
appended (same EmployeeKey, same is_current/Startdate/Enddate/attrs, just
with the two photo columns added and a bumped version), exactly like the
Phase 2 SCD jobs and the Phase 4 streaming consumer do.

Run once via a throwaway container on nw_de_net (same pattern as
generate_avatars.py):
    docker run --rm --network nw_de_net -v $(pwd)/datalake:/app -w /app \
      -e CH_HOST=clickhouse -e CH_PASSWORD=... \
      python:3.11-slim bash -c "pip install --quiet clickhouse-connect && python link_photos_to_dw.py"
"""
import json
import os
import time

import clickhouse_connect

CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CH_PORT", "8123"))
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DATABASE = os.environ.get("CH_DATABASE", "NorthwindDW")


def main():
    with open("/app/photo_catalog.json") as f:
        catalog = json.load(f)

    ch = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DATABASE,
    )

    # ---- 1. Load the metadata catalog (lakehouse metadata layer) ----
    catalog_cols = ["EmployeeCode", "EmployeeID", "ObjectKey", "Url", "ContentType", "SizeBytes"]
    catalog_rows = [
        [r["employee_code"], r["employee_id"], r["object_key"], r["url"], r["content_type"], r["size_bytes"]]
        for r in catalog
    ]
    ch.insert("employee_photo_catalog", catalog_rows, column_names=catalog_cols)
    print(f"employee_photo_catalog: inserted {len(catalog_rows)} rows")

    # ---- 2. Backfill PhotoObjectKey/PhotoUrl onto current DimEmployees rows ----
    url_by_employee_id = {r["employee_id"]: (r["object_key"], r["url"]) for r in catalog}

    res = ch.query("DESCRIBE TABLE DimEmployees")
    all_cols = [row[0] for row in res.result_rows]
    data_cols = [c for c in all_cols if c not in ("PhotoObjectKey", "PhotoUrl", "version")]

    current = ch.query(
        f"SELECT {','.join(all_cols)} FROM DimEmployees FINAL WHERE is_current = 1"
    )
    version = int(time.time() * 1000)

    updated = 0
    for row in current.result_rows:
        row_dict = dict(zip(all_cols, row))
        employee_id = row_dict["EmployeeID"]
        if employee_id not in url_by_employee_id:
            continue
        object_key, url = url_by_employee_id[employee_id]
        row_dict["PhotoObjectKey"] = object_key
        row_dict["PhotoUrl"] = url
        row_dict["version"] = version
        ch.insert("DimEmployees", [[row_dict[c] for c in all_cols]], column_names=all_cols)
        updated += 1

    print(f"DimEmployees: backfilled PhotoObjectKey/PhotoUrl on {updated} rows (version={version})")


if __name__ == "__main__":
    main()
