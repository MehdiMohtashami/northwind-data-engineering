from datetime import datetime, timezone

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

MSSQL_CONN_ID = "mssql_northwind"
PG_CONN_ID = "pg_staging"

MAPPINGS = [
    {
        "key": "suppliers",
        "staging_table": "stg_suppliers",
        "sql": """
            SELECT
                SupplierID   AS supplier_id,
                CompanyName  AS company_name,
                ContactName  AS contact_name,
                ContactTitle AS contact_title,
                Address      AS address,
                City         AS city,
                Region       AS region,
                PostalCode   AS postal_code,
                Country      AS country,
                Phone        AS phone,
                Fax          AS fax,
                CAST(HomePage AS NVARCHAR(MAX)) AS homepage
            FROM dbo.Suppliers;
        """,
    },
    {
        "key": "products",
        "staging_table": "stg_products",
        "sql": """
            SELECT
                ProductID       AS product_id,
                ProductName     AS product_name,
                SupplierID      AS supplier_id,
                CategoryID      AS category_id,
                QuantityPerUnit AS quantity_per_unit,
                UnitPrice       AS unit_price,
                UnitsInStock    AS units_in_stock,
                UnitsOnOrder    AS units_on_order,
                ReorderLevel    AS reorder_level,
                Discontinued    AS discontinued
            FROM dbo.Products;
        """,
        "bool_columns": ["discontinued"],
    },
    {
        "key": "categories",
        "staging_table": "stg_categories",
        "sql": """
            SELECT
                CategoryID   AS category_id,
                CategoryName AS category_name,
                CAST(Description AS NVARCHAR(MAX)) AS description,
                Picture      AS picture
            FROM dbo.Categories;
        """,
        "binary_columns": ["picture"],
    },
    {
        "key": "customers",
        "staging_table": "stg_customers",
        "sql": """
            SELECT
                RTRIM(CustomerID) AS customer_id,
                CompanyName       AS company_name,
                ContactName       AS contact_name,
                ContactTitle      AS contact_title,
                Address           AS address,
                City              AS city,
                Region            AS region,
                PostalCode        AS postal_code,
                Country           AS country,
                Phone             AS phone,
                Fax               AS fax
            FROM dbo.Customers;
        """,
    },
    {
        "key": "employees",
        "staging_table": "stg_employees",
        "sql": """
            SELECT
                EmployeeID      AS employee_id,
                LastName        AS last_name,
                FirstName       AS first_name,
                Title           AS title,
                TitleOfCourtesy AS title_of_courtesy,
                BirthDate       AS birth_date,
                HireDate        AS hire_date,
                Address         AS address,
                City            AS city,
                Region          AS region,
                PostalCode      AS postal_code,
                Country         AS country,
                HomePhone       AS home_phone,
                Extension       AS extension,
                Photo           AS photo,
                CAST(Notes AS NVARCHAR(MAX)) AS notes,
                ReportsTo       AS reports_to,
                PhotoPath       AS photo_path
            FROM dbo.Employees;
        """,
        "binary_columns": ["photo"],
    },
    {
        "key": "shippers",
        "staging_table": "stg_shippers",
        "sql": """
            SELECT
                ShipperID   AS shipper_id,
                CompanyName AS company_name,
                Phone       AS phone
            FROM dbo.Shippers;
        """,
    },
    {
        "key": "territories",
        "staging_table": "stg_territories",
        "sql": """
            SELECT
                t.TerritoryID AS territory_id,
                RTRIM(t.TerritoryDescription) AS territory_description,
                t.RegionID    AS region_id
            FROM dbo.Territories t
            JOIN dbo.Region r ON t.RegionID = r.RegionID;
        """,
    },
    {
        "key": "employee_territories",
        "staging_table": "stg_employee_territories",
        "sql": """
            SELECT
                EmployeeID  AS employee_id,
                TerritoryID AS territory_id
            FROM dbo.EmployeeTerritories;
        """,
    },
    {
        "key": "orders",
        "staging_table": "stg_orders",
        "sql": """
            SELECT
                OrderID           AS order_id,
                RTRIM(CustomerID) AS customer_id,
                EmployeeID        AS employee_id,
                OrderDate         AS order_date,
                RequiredDate      AS required_date,
                ShippedDate       AS shipped_date,
                ShipVia           AS ship_via,
                Freight           AS freight,
                ShipName          AS ship_name,
                ShipAddress       AS ship_address,
                ShipCity          AS ship_city,
                ShipRegion        AS ship_region,
                ShipPostalCode    AS ship_postal_code,
                ShipCountry       AS ship_country
            FROM dbo.Orders;
        """,
    },
    {
        "key": "order_details",
        "staging_table": "stg_order_details",
        "sql": """
            SELECT
                OrderID   AS order_id,
                ProductID AS product_id,
                UnitPrice AS unit_price,
                Quantity  AS quantity,
                Discount  AS discount
            FROM dbo.[Order Details];
        """,
    },
    {
        "key": "geography",
        "staging_table": "stg_geography",
        "sql": """
            SELECT City AS city, Region AS region, Country AS country, PostalCode AS postal_code
            FROM dbo.Customers WHERE City IS NOT NULL
            UNION
            SELECT City, Region, Country, PostalCode
            FROM dbo.Employees WHERE City IS NOT NULL
            UNION
            SELECT City, Region, Country, PostalCode
            FROM dbo.Suppliers WHERE City IS NOT NULL
            UNION
            SELECT ShipCity, ShipRegion, ShipCountry, ShipPostalCode
            FROM dbo.Orders WHERE ShipCity IS NOT NULL;
        """,
    },
]


def _extract_and_load(mapping: dict) -> None:
    from sqlalchemy.types import LargeBinary

    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    staging_table = mapping["staging_table"]
    started_at = datetime.now(timezone.utc)
    status = "success"
    message = None
    row_count = 0

    try:
        df = mssql_hook.get_pandas_df(mapping["sql"])
        row_count = len(df)

        for col in mapping.get("bool_columns", []):
            df[col] = df[col].astype(bool)

        pg_hook.run(f"TRUNCATE TABLE staging.{staging_table};")

        dtype = {col: LargeBinary() for col in mapping.get("binary_columns", [])}
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql(
            staging_table,
            engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=200,
            dtype=dtype or None,
        )
    except Exception as exc:
        status = "failed"
        message = str(exc)[:2000]
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        pg_hook.run(
            """
            INSERT INTO staging.load_log
                (table_name, load_type, rows_loaded, started_at, finished_at, status, message)
            VALUES
                (%(table_name)s, %(load_type)s, %(rows_loaded)s, %(started_at)s, %(finished_at)s, %(status)s, %(message)s)
            """,
            parameters={
                "table_name": staging_table,
                "load_type": "full_truncate_load",
                "rows_loaded": row_count,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "message": message,
            },
        )


with DAG(
    dag_id="dim_extract_to_staging",
    description="Full truncate+load extraction from Northwind OLTP into Postgres staging landing tables.",
    schedule="0 22 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["phase1", "extract"],
) as dag:

    with TaskGroup(group_id="extract_to_staging") as extract_group:
        for mapping in MAPPINGS:

            @task(task_id=mapping["key"])
            def run_mapping(m=mapping):
                _extract_and_load(m)

            run_mapping()
