# Northwind Data Engineering — Phase 0

Isolated Docker Compose stack for the Northwind data-engineering final project.
Compose project name: `nw_de`. Network: `nw_de_net`. All named volumes are
project-scoped (`nw_de_<volume>`), so this stack is fully isolated from any
other Docker workloads on the host.

## Prerequisites

- Docker + Docker Compose v2
- The three backup files placed in `backups/` (git-ignored):
  - `Northwind.bak` — OLTP source (`Northwind` DB, the CDC source)
  - `instnwnd.sql` — fallback OLTP install script (unused unless `.bak` restore fails)
  - `Northwind_DW_1404_08_14_Full.bak` — professor's reference DW backup

## Up / Down

```bash
# start everything
docker compose up -d

# check status
docker compose ps

# stop everything (keeps volumes/data)
docker compose down

# stop and wipe all data volumes (destructive, this project's volumes only)
docker compose down -v
```

### First-time SQL Server restore (manual, one-time)

The `sqlserver` service does not auto-restore backups. After it reports
healthy:

```bash
# 1) Restore the OLTP source
docker exec nw_de_sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -N -Q "
RESTORE DATABASE Northwind
FROM DISK = N'/backups/Northwind.bak'
WITH MOVE 'Northwind'     TO '/var/opt/mssql/data/northwnd.mdf',
     MOVE 'Northwind_log' TO '/var/opt/mssql/data/northwnd_log.ldf',
     REPLACE;"

# 2) Restore the professor's reference DW (logical names discovered via RESTORE FILELISTONLY)
docker exec nw_de_sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -N -Q "
RESTORE DATABASE Northwind_DW
FROM DISK = N'/backups/Northwind_DW_1404_08_14_Full.bak'
WITH MOVE 'Northwind_BI_1404_05_DW'     TO '/var/opt/mssql/data/Northwind_DW.mdf',
     MOVE 'Northwind_BI_1404_05_DW_log' TO '/var/opt/mssql/data/Northwind_DW_log.ldf',
     REPLACE;"

# 3) Create ETL_Settings and enable CDC on Northwind (run AFTER step 1)
docker exec nw_de_sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -N \
  -i /docker-entrypoint-initdb.d/01_etl_settings_and_cdc.sql
```

## Ports

| Service            | Host port(s)     | Notes                                   |
|---------------------|------------------|------------------------------------------|
| SQL Server           | 1433             | OLTP `Northwind`, `Northwind_DW`, `ETL_Settings` |
| Postgres (staging)    | 5433             | maps to container 5432                  |
| ClickHouse            | 8123 (HTTP), 9000 (native) | `NorthwindDW` database        |
| Kafka                 | 9092             | internal listener `kafka:29092`         |
| Zookeeper              | (internal only) | 2181, not exposed to host                |
| Airflow webserver       | 8080            | LocalExecutor, metadata DB is internal-only `postgres_airflow` |
| Spark master UI          | 8081           | maps to container 8080                   |
| Spark master RPC          | 7077          |                                            |
| Grafana                    | 3000         | ClickHouse datasource pre-provisioned      |

Host ports 5432 (Postgres), 6379 (Redis), and 3306 (MySQL) are intentionally
**not** used — those belong to the pre-existing `postgres_django`,
`redis_django`, and `local-mysql` containers from a different project and are
never touched by this stack.

## Default accounts

| Service     | User      | Password         |
|-------------|-----------|------------------|
| SQL Server  | sa        | see `MSSQL_SA_PASSWORD` in `.env` |
| Postgres staging | see `PG_STAGING_USER` in `.env` | see `PG_STAGING_PASSWORD` in `.env` |
| ClickHouse  | default   | see `CLICKHOUSE_PASSWORD` in `.env` |
| Airflow     | admin     | admin            |
| Grafana     | admin     | admin (default, change on first login) |

## Layout

```
infra/sqlserver/init/     -- ETL_Settings schema + CDC enable script (run manually, see above)
infra/postgres/init/      -- staging schema (auto-runs on first postgres_staging boot)
infra/clickhouse/init/    -- NorthwindDW star schema + DimDate seed (auto-runs on first clickhouse boot)
infra/grafana/provisioning/ -- ClickHouse datasource provisioning
airflow/{dags,plugins,logs}/ -- empty, DAGs come in a later phase
spark/                    -- empty, Spark jobs come in a later phase
streaming/{producer,consumer}/ -- empty, Kafka producer/consumer come in a later phase
sql/                      -- ad-hoc SQL / reconciliation notes
docs/                     -- project docs
```

## Notes / known deviations

- `bitnami/spark:3.5` was removed from Docker Hub; the stack uses
  `bitnamilegacy/spark:3.5` instead (same version, legacy registry).
- The professor's reference `Northwind_DW` (restored into SQL Server for
  inspection) does not have a separate `DimShipName` table — `ShipName` is a
  degenerate attribute directly on `FactOrders`, and it also has
  `DimTerritories` / `FactEmployeeTerritories`, which are not part of our
  locked ClickHouse star schema. See project chat history / `docs/` for the
  full reconciliation if you want to adjust the ClickHouse DDL to match.
