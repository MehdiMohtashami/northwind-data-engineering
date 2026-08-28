-- Staging schema: landing tables for full/truncate-and-load from the Northwind OLTP source.
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_geography (
    geography_key   SERIAL PRIMARY KEY,
    city            VARCHAR(100),
    region          VARCHAR(100),
    country         VARCHAR(100),
    postal_code     VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS staging.stg_categories (
    category_id     INT PRIMARY KEY,
    category_name   VARCHAR(100),
    description     TEXT,
    picture         BYTEA
);

CREATE TABLE IF NOT EXISTS staging.stg_suppliers (
    supplier_id     INT PRIMARY KEY,
    company_name    VARCHAR(200),
    contact_name    VARCHAR(100),
    contact_title   VARCHAR(100),
    address         VARCHAR(200),
    city            VARCHAR(100),
    region          VARCHAR(100),
    postal_code     VARCHAR(20),
    country         VARCHAR(100),
    phone           VARCHAR(50),
    fax             VARCHAR(50),
    homepage        TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id       INT PRIMARY KEY,
    product_name     VARCHAR(200),
    supplier_id      INT,
    category_id      INT,
    quantity_per_unit VARCHAR(100),
    unit_price       NUMERIC(19,4),
    units_in_stock   SMALLINT,
    units_on_order   SMALLINT,
    reorder_level    SMALLINT,
    discontinued     BOOLEAN
);

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id     VARCHAR(10) PRIMARY KEY,
    company_name    VARCHAR(200),
    contact_name    VARCHAR(100),
    contact_title   VARCHAR(100),
    address         VARCHAR(200),
    city            VARCHAR(100),
    region          VARCHAR(100),
    postal_code     VARCHAR(20),
    country         VARCHAR(100),
    phone           VARCHAR(50),
    fax             VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS staging.stg_employees (
    employee_id      INT PRIMARY KEY,
    last_name        VARCHAR(100),
    first_name       VARCHAR(100),
    title            VARCHAR(100),
    title_of_courtesy VARCHAR(50),
    birth_date       DATE,
    hire_date        DATE,
    address          VARCHAR(200),
    city             VARCHAR(100),
    region           VARCHAR(100),
    postal_code      VARCHAR(20),
    country          VARCHAR(100),
    home_phone       VARCHAR(50),
    extension        VARCHAR(20),
    photo            BYTEA,
    notes            TEXT,
    reports_to       INT,
    photo_path       VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS staging.stg_shippers (
    shipper_id      INT PRIMARY KEY,
    company_name    VARCHAR(200),
    phone           VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS staging.stg_territories (
    territory_id           VARCHAR(20) PRIMARY KEY,
    territory_description  VARCHAR(100),
    region_id              INT
);

CREATE TABLE IF NOT EXISTS staging.stg_employee_territories (
    employee_id     INT,
    territory_id    VARCHAR(20),
    PRIMARY KEY (employee_id, territory_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id         INT PRIMARY KEY,
    customer_id      VARCHAR(10),
    employee_id      INT,
    order_date       DATE,
    required_date    DATE,
    shipped_date     DATE,
    ship_via         INT,
    freight          NUMERIC(19,4),
    ship_name        VARCHAR(200),
    ship_address     VARCHAR(200),
    ship_city        VARCHAR(100),
    ship_region      VARCHAR(100),
    ship_postal_code VARCHAR(20),
    ship_country     VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS staging.stg_order_details (
    order_id        INT,
    product_id      INT,
    unit_price      NUMERIC(19,4),
    quantity        SMALLINT,
    discount        REAL,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE IF NOT EXISTS staging.load_log (
    log_id          SERIAL PRIMARY KEY,
    table_name      VARCHAR(128) NOT NULL,
    load_type       VARCHAR(20) NOT NULL,
    rows_loaded     INT,
    started_at      TIMESTAMP NOT NULL DEFAULT now(),
    finished_at     TIMESTAMP,
    status          VARCHAR(20),
    message         TEXT
);
