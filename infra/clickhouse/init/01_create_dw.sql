CREATE DATABASE IF NOT EXISTS NorthwindDW;

-- ============ DIMENSIONS ============

-- SCD2 dims (type2 in ETL_Settings.table_config): full history via is_current + Startdate/Enddate.

CREATE TABLE IF NOT EXISTS NorthwindDW.DimGeography
(
    GeographyKey UInt32,
    City         String,
    Region       Nullable(String),
    Country      String,
    PostalCode   Nullable(String),
    is_current   UInt8 DEFAULT 1,
    Startdate    Date DEFAULT toDate('1970-01-01'),
    Enddate      Date DEFAULT toDate('2099-12-31'),
    version      UInt32,
    is_inferred  UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY GeographyKey;

CREATE TABLE IF NOT EXISTS NorthwindDW.DimSuppliers
(
    SupplierKey   UInt32,
    SupplierID    Int32,
    CompanyName   String,
    ContactName   Nullable(String),
    ContactTitle  Nullable(String),
    Address       Nullable(String),
    Phone         Nullable(String),
    GeographyKey  UInt32,
    is_current    UInt8 DEFAULT 1,
    Startdate     Date DEFAULT toDate('1970-01-01'),
    Enddate       Date DEFAULT toDate('2099-12-31'),
    version       UInt64  -- widened from UInt32 in Phase 4: the streaming delete
                          -- proof-of-concept writes LSN-derived versions here too.
)
ENGINE = ReplacingMergeTree(version)
ORDER BY SupplierKey;

CREATE TABLE IF NOT EXISTS NorthwindDW.DimProducts
(
    ProductKey       UInt32,
    ProductID        Int32,
    ProductName      String,
    SupplierKey      UInt32,
    CategoryID       Nullable(Int32),
    CategoryName     Nullable(String),
    QuantityPerUnit  Nullable(String),
    UnitPrice        Decimal(19,4),
    Discontinued     UInt8,
    is_current       UInt8 DEFAULT 1,
    Startdate        Date DEFAULT toDate('1970-01-01'),
    Enddate          Date DEFAULT toDate('2099-12-31'),
    version          UInt32,
    is_inferred      UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY ProductKey;

CREATE TABLE IF NOT EXISTS NorthwindDW.DimCustomer
(
    CustomerKey   UInt32,
    CustomerID    String,
    CompanyName   String,
    ContactName   Nullable(String),
    ContactTitle  Nullable(String),
    Phone         Nullable(String),
    GeographyKey  UInt32,
    is_current    UInt8 DEFAULT 1,
    Startdate     Date DEFAULT toDate('1970-01-01'),
    Enddate       Date DEFAULT toDate('2099-12-31'),
    version       UInt32,
    is_inferred   UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY CustomerKey;

CREATE TABLE IF NOT EXISTS NorthwindDW.DimEmployees
(
    EmployeeKey      UInt32,
    EmployeeID       Int32,
    LastName         String,
    FirstName        String,
    Title            Nullable(String),
    TitleOfCourtesy  Nullable(String),
    BirthDate        Nullable(Date32),  -- Date32 (not Date) since Northwind employees were born before 1970
    HireDate         Nullable(Date),
    GeographyKey     UInt32,
    ManagerKey       Nullable(UInt32),
    is_current       UInt8 DEFAULT 1,
    Startdate        Date DEFAULT toDate('1970-01-01'),
    Enddate          Date DEFAULT toDate('2099-12-31'),
    version          UInt32,
    is_inferred      UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY EmployeeKey;

CREATE TABLE IF NOT EXISTS NorthwindDW.DimTerritories
(
    TerritoryKey          UInt32,
    TerritoryID           String,
    RegionDescription     Nullable(String),
    TerritoryDescription  String,
    is_current            UInt8 DEFAULT 1,
    Startdate             Date DEFAULT toDate('1970-01-01'),
    Enddate               Date DEFAULT toDate('2099-12-31'),
    version               UInt32
)
ENGINE = ReplacingMergeTree(version)
ORDER BY TerritoryKey;

-- SCD1 dims (type1 in ETL_Settings.table_config): overwrite in place via version bump, no history columns.

CREATE TABLE IF NOT EXISTS NorthwindDW.DimShippers
(
    ShipperKey   UInt32,
    ShipperID    Int32,
    CompanyName  String,
    Phone        Nullable(String),
    version      UInt32,
    is_inferred  UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(version)
ORDER BY ShipperKey;

-- Type0 (static, no versioning needed)
CREATE TABLE IF NOT EXISTS NorthwindDW.DimDate
(
    DateKey     UInt32,
    FullDate    Date,
    Day         UInt8,
    Month       UInt8,
    MonthName   String,
    Quarter     UInt8,
    Year        UInt16,
    DayOfWeek   UInt8,
    DayName     String,
    IsWeekend   UInt8
)
ENGINE = MergeTree
ORDER BY DateKey;

INSERT INTO NorthwindDW.DimDate
SELECT
    toUInt32(formatDateTime(d, '%Y%m%d'))  AS DateKey,
    d                                       AS FullDate,
    toUInt8(toDayOfMonth(d))                AS Day,
    toUInt8(toMonth(d))                     AS Month,
    formatDateTime(d, '%M')                 AS MonthName,
    toUInt8(toQuarter(d))                   AS Quarter,
    toUInt16(toYear(d))                     AS Year,
    toUInt8(toDayOfWeek(d))                 AS DayOfWeek,
    formatDateTime(d, '%W')                 AS DayName,
    if(toDayOfWeek(d) IN (6, 7), 1, 0)       AS IsWeekend
FROM
(
    SELECT toDate('1990-01-01') + number AS d
    FROM numbers(14976)
)
WHERE d <= toDate('2030-12-31');

-- ============ FACT ============

CREATE TABLE IF NOT EXISTS NorthwindDW.FactOrders
(
    OrderID          Int32,
    ProductKey       UInt32,
    CustomerKey      UInt32,
    EmployeeKey      UInt32,
    ShipperKey       UInt32,
    GeographyKey     UInt32 DEFAULT 0,   -- ship-to geography
    ShipName         Nullable(String),   -- degenerate dimension, no DimShipName (matches professor's design)
    OrderDateKey     UInt32,
    RequiredDateKey  Nullable(UInt32),
    ShippedDateKey   Nullable(UInt32),
    UnitPrice        Decimal(19,4),
    Quantity         Int16,
    Discount         Float32,
    Freight          Nullable(Decimal(19,4)),
    is_deleted       UInt8 DEFAULT 0,
    version          UInt64  -- widened from UInt32 in Phase 4: streaming writes use the
                              -- first 8 bytes of the CDC LSN (big-endian) as version, so
                              -- replays are truly idempotent (same LSN -> same version).
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (OrderID, ProductKey);

-- ============ FACTLESS BRIDGE ============

CREATE TABLE IF NOT EXISTS NorthwindDW.FactEmployeeTerritories
(
    EmployeeKey   UInt32,
    TerritoryKey  UInt32,
    is_deleted    UInt8 DEFAULT 0,
    version       UInt32
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (EmployeeKey, TerritoryKey);

-- ============ FLAT VIEW FOR GRAFANA ============

CREATE VIEW IF NOT EXISTS NorthwindDW.v_FactOrders_Flat AS
SELECT
    f.OrderID,
    f.OrderDateKey,
    dd.FullDate      AS OrderDate,
    dd.Year          AS OrderYear,
    dd.Quarter       AS OrderQuarter,
    dd.MonthName     AS OrderMonthName,
    p.ProductID,
    p.ProductName,
    p.CategoryName,
    c.CustomerID,
    c.CompanyName    AS CustomerName,
    e.EmployeeID,
    concat(e.FirstName, ' ', e.LastName) AS EmployeeName,
    s.ShipperID,
    s.CompanyName    AS ShipperName,
    f.ShipName,
    f.UnitPrice AS UnitPrice,
    f.Quantity,
    f.Discount,
    f.Freight,
    (f.UnitPrice * f.Quantity * (1 - f.Discount)) AS LineTotal
FROM
(
    SELECT * FROM NorthwindDW.FactOrders FINAL WHERE is_deleted = 0
) AS f
LEFT JOIN (SELECT * FROM NorthwindDW.DimProducts   FINAL) AS p  ON f.ProductKey  = p.ProductKey
LEFT JOIN (SELECT * FROM NorthwindDW.DimCustomer   FINAL) AS c  ON f.CustomerKey = c.CustomerKey
LEFT JOIN (SELECT * FROM NorthwindDW.DimEmployees  FINAL) AS e  ON f.EmployeeKey = e.EmployeeKey
LEFT JOIN (SELECT * FROM NorthwindDW.DimShippers   FINAL) AS s  ON f.ShipperKey  = s.ShipperKey
LEFT JOIN (SELECT * FROM NorthwindDW.DimDate)             AS dd ON f.OrderDateKey = dd.DateKey;
