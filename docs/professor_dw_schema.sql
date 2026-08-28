-- Reference schema export of the professor's data warehouse.
-- Database name in our restored SQL Server instance: Northwind_DW
--   (the backup's logical file name is "Northwind_BI_1404_05_DW" / "_log" —
--    that is a FILE name, not the database name; we restored the DB as
--    Northwind_DW in Phase 0. This script documents Northwind_DW as it
--    exists today, reverse-engineered from sys.tables/sys.columns/sys.foreign_keys.)
-- Row counts as of Phase 1 export are noted per table.

-- ============ DimGeography (150 rows) ============
CREATE TABLE dbo.DimGeography (
    GeographyKey INT           NOT NULL PRIMARY KEY,   -- identity
    Country      NVARCHAR(30)  NULL,
    Region       NVARCHAR(30)  NULL,
    City         NVARCHAR(30)  NULL,
    PostalCode   NVARCHAR(20)  NULL,
    Address      NVARCHAR(120) NULL
);

-- ============ DimSuppliers (33 rows) ============
CREATE TABLE dbo.DimSuppliers (
    SupplierKey          INT           NOT NULL PRIMARY KEY,  -- identity
    SupplierAlternateKey INT           NULL,                  -- natural SupplierID
    GeographyKey         INT           NULL REFERENCES dbo.DimGeography(GeographyKey),
    CompanyName           NVARCHAR(80)  NULL,
    ContactName            NVARCHAR(60)  NULL,
    ContactTitle            NVARCHAR(60)  NULL,
    Phone                    NVARCHAR(48)  NULL,
    Fax                       NVARCHAR(48)  NULL,
    HomePage                  NVARCHAR(MAX) NULL,
    Startdate                 DATETIME      NULL,   -- SCD2
    Enddate                    DATETIME      NULL    -- SCD2
);

-- ============ DimProducts (81 rows) ============
CREATE TABLE dbo.DimProducts (
    ProductKey        INT           NOT NULL PRIMARY KEY,  -- identity
    ProductAlternateKey INT         NOT NULL,              -- natural ProductID
    SupplierKey        INT           NULL REFERENCES dbo.DimSuppliers(SupplierKey),
    ProductName          NVARCHAR(80)  NULL,
    CategoryName          NVARCHAR(30)  NULL,               -- denormalized, no separate DimCategories
    QuantityPerUnit         NVARCHAR(40)  NULL,
    UnitPrice                 MONEY         NULL,
    UnitsInStock                SMALLINT      NULL,
    UnitsOnOrder                  SMALLINT      NULL,
    ReorderLevel                    SMALLINT      NULL,
    Discontinued                      BIT           NULL,
    Startdate                          DATETIME      NOT NULL,  -- SCD2
    Enddate                              DATETIME      NULL      -- SCD2
);

-- ============ DimCustomer (91 rows) ============
CREATE TABLE dbo.DimCustomer (
    CustomerKey          INT           NOT NULL PRIMARY KEY,  -- identity
    CustomerAlternateKey NVARCHAR(10)  NULL,                  -- natural CustomerID
    GeographyKey          INT           NULL REFERENCES dbo.DimGeography(GeographyKey),
    CompanyName             NVARCHAR(80)  NULL,
    ContactName               NVARCHAR(60)  NULL,
    ContactTitle                NVARCHAR(60)  NULL,
    Phone                         NVARCHAR(48)  NULL,
    Fax                             NVARCHAR(48)  NULL,
    Startdate                        DATETIME      NOT NULL,  -- SCD2
    Enddate                            DATETIME      NULL      -- SCD2
);

-- ============ DimEmployees (9 rows) ============
-- NOTE: table is named DimEmployees (plural), not DimEmployee.
-- Has BOTH a surrogate self-reference (ParentEmployeeKey, FK to this table's
-- own EmployeeKey) AND the raw natural-key column from the source (ReportsTo).
-- ParentEmployeeKey is the one that plays the role of "ManagerKey" in our
-- ClickHouse design.
CREATE TABLE dbo.DimEmployees (
    EmployeeKey         INT           NOT NULL PRIMARY KEY,  -- identity
    ParentEmployeeKey    INT           NULL REFERENCES dbo.DimEmployees(EmployeeKey),  -- surrogate manager FK
    EmployeeAlternateKey  INT           NULL,                 -- natural EmployeeID
    ReportsTo               INT           NULL,                 -- natural EmployeeID of manager (source column, unresolved)
    GeographyKey              INT           NULL REFERENCES dbo.DimGeography(GeographyKey),
    FirstName                   NVARCHAR(20)  NULL,
    LastName                      NVARCHAR(40)  NULL,
    Title                           NVARCHAR(60)  NULL,
    TitleOfCourtesy                   NVARCHAR(50)  NULL,
    BirthDate                           DATETIME      NULL,
    HireDate                              DATETIME      NULL,
    HomePhone                               NVARCHAR(48)  NULL,
    Extension                                 NVARCHAR(8)   NULL,
    Photo                                       VARBINARY(MAX) NULL,
    Notes                                         NVARCHAR(MAX) NULL,
    PhotoPath                                       NVARCHAR(510) NULL,
    Startdate                                         DATETIME      NOT NULL,  -- SCD2
    Enddate                                             DATETIME      NULL      -- SCD2
);

-- ============ DimShippers (3 rows) ============
-- No SCD2 columns (Startdate/Enddate) — professor treats this as SCD1/static.
CREATE TABLE dbo.DimShippers (
    ShipperKey          INT          NOT NULL PRIMARY KEY,  -- identity
    ShipperAlternateKey  INT          NULL,                 -- natural ShipperID
    CompanyName            NVARCHAR(80) NULL,
    Phone                    NVARCHAR(48) NULL
);

-- ============ DimTerritories (53 rows) ============
-- Region is denormalized as RegionDescription (no separate DimRegion table).
CREATE TABLE dbo.DimTerritories (
    TerritoryKey          INT           NOT NULL PRIMARY KEY,  -- identity
    TerritoryAlternateKey  NVARCHAR(40)  NULL,                 -- natural TerritoryID
    RegionDescription        NVARCHAR(100) NULL,
    TerritoryDescription       NVARCHAR(100) NULL,
    Startdate                    DATETIME      NOT NULL,  -- SCD2
    Enddate                        DATETIME      NULL      -- SCD2
);

-- ============ FactEmployeeTerritories (49 rows) — factless bridge ============
-- Grain: one row per (employee, territory) assignment. No measures.
CREATE TABLE dbo.FactEmployeeTerritories (
    EmployeeKey  INT NOT NULL REFERENCES dbo.DimEmployees(EmployeeKey),
    TerritoryKey INT NOT NULL REFERENCES dbo.DimTerritories(TerritoryKey),
    PRIMARY KEY (EmployeeKey, TerritoryKey)
);

-- ============ DimDate (4017 rows) ============
CREATE TABLE dbo.DimDate (
    DateKey             INT          NOT NULL PRIMARY KEY,  -- yyyymmdd, not identity
    FullDateAlternateKey DATE         NULL,
    CalendarYear           SMALLINT     NULL,
    CalendarSeason           TINYINT      NULL,
    SeasonName                 NVARCHAR(20) NULL,
    MonthNumberOfYear            TINYINT      NULL,
    MonthName                       NVARCHAR(20) NOT NULL,
    DayNumberOfMonth                   TINYINT      NULL,
    DayOfWeek                             SMALLINT     NOT NULL,
    DayOfWeekName                           NVARCHAR(60) NOT NULL
);

-- ============ FactOrders (2154 rows) ============
-- Grain: one row per (OrderID, ProductKey) — i.e. one row per order line.
-- IMPORTANT: ShipName is a plain (degenerate) column here, NOT a separate
-- dimension. There is no DimShipName table in the professor's design.
-- GeographyKey on the fact is the SHIP-TO geography (independent of
-- CustomerKey's own geography). Order/Required/Shipped dates are stored
-- BOTH as surrogate *DateKey columns AND as raw datetime columns
-- (denormalized convenience columns alongside the DimDate FKs).
-- Measures: Freight, UnitPrice, Quantity, Discount.
CREATE TABLE dbo.FactOrders (
    OrderID          INT     NOT NULL,
    GeographyKey     INT     NULL REFERENCES dbo.DimGeography(GeographyKey),   -- ship-to geography
    ProductKey       INT     NOT NULL REFERENCES dbo.DimProducts(ProductKey),
    CustomerKey      INT     NULL REFERENCES dbo.DimCustomer(CustomerKey),
    EmployeeKey      INT     NULL REFERENCES dbo.DimEmployees(EmployeeKey),
    ShipperKey       INT     NULL REFERENCES dbo.DimShippers(ShipperKey),
    OrderdateKey     INT     NULL REFERENCES dbo.DimDate(DateKey),
    RequiredDateKey  INT     NULL REFERENCES dbo.DimDate(DateKey),
    ShippedDateKey   INT     NULL REFERENCES dbo.DimDate(DateKey),
    Freight          MONEY   NULL,
    ShipName         NVARCHAR(80) NULL,   -- degenerate dimension, no DimShipName
    UnitPrice        MONEY   NULL,
    Quantity         SMALLINT NULL,
    Discount         REAL    NULL,
    OrderDate        DATETIME NULL,        -- denormalized alongside OrderdateKey
    ShippedDate      DATETIME NULL,        -- denormalized alongside ShippedDateKey
    RequiredDate     DATETIME NULL,        -- denormalized alongside RequiredDateKey
    PRIMARY KEY (OrderID, ProductKey)
);

-- ============ Foreign keys (as found in sys.foreign_keys) ============
-- FK_DimCustomer_DimGeography              DimCustomer.GeographyKey -> DimGeography.GeographyKey
-- FK_DimEmployees_DimGeography             DimEmployees.GeographyKey -> DimGeography.GeographyKey
-- FK_DimEmployees_DimEmployees             DimEmployees.ParentEmployeeKey -> DimEmployees.EmployeeKey
-- FK_DimProducts_DimSuppliers              DimProducts.SupplierKey -> DimSuppliers.SupplierKey
-- FK_DimSuppliers_DimGeography             DimSuppliers.GeographyKey -> DimGeography.GeographyKey
-- FK_FactEmployeeTerritories_DimEmployees  FactEmployeeTerritories.EmployeeKey -> DimEmployees.EmployeeKey
-- FK_FactEmployeeTerritories_DimTerritories FactEmployeeTerritories.TerritoryKey -> DimTerritories.TerritoryKey
-- FK_FactOrders_DimDate / _DimDate1 / _DimDate2   FactOrders.{OrderdateKey,RequiredDateKey,ShippedDateKey} -> DimDate.DateKey
-- FK_FactOrders_DimShippers                FactOrders.ShipperKey -> DimShippers.ShipperKey
-- FK_FactOrders_DimEmployees                FactOrders.EmployeeKey -> DimEmployees.EmployeeKey
-- FK_FactOrders_DimProducts                 FactOrders.ProductKey -> DimProducts.ProductKey
-- FK_FactOrders_DimCustomer                 FactOrders.CustomerKey -> DimCustomer.CustomerKey
-- FK_FactOrders_DimGeography                FactOrders.GeographyKey -> DimGeography.GeographyKey

-- There is also a dbo.sysdiagrams table (1 row) — a SSMS database-diagram
-- system table, not part of the logical model. Not reproduced here.
