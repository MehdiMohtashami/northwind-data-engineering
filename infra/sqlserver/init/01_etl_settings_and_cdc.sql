-- Run this AFTER Northwind has been restored (CDC enable requires the source DB to exist).

USE master;
GO
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ETL_Settings')
BEGIN
    CREATE DATABASE ETL_Settings;
END
GO

USE ETL_Settings;
GO

IF OBJECT_ID('dbo.cdc_state') IS NULL
BEGIN
    CREATE TABLE dbo.cdc_state (
        cdc_state_id       INT IDENTITY(1,1) PRIMARY KEY,
        source_table       VARCHAR(128) NOT NULL UNIQUE,
        capture_instance   VARCHAR(128) NOT NULL,
        last_lsn           VARBINARY(10) NULL,
        last_processed_at  DATETIME2 NULL,
        -- Phase 4: separate watermark for the Kafka streaming producer, so the
        -- (paused) batch DAG's last_lsn and the stream never fight over the
        -- same cursor.
        stream_last_lsn    VARBINARY(10) NULL
    );
END
GO

IF COL_LENGTH('dbo.cdc_state', 'stream_last_lsn') IS NULL
BEGIN
    ALTER TABLE dbo.cdc_state ADD stream_last_lsn VARBINARY(10) NULL;
END
GO

IF OBJECT_ID('dbo.table_config') IS NULL
BEGIN
    CREATE TABLE dbo.table_config (
        table_config_id INT IDENTITY(1,1) PRIMARY KEY,
        table_name      VARCHAR(128) NOT NULL UNIQUE,
        table_type      VARCHAR(20) NOT NULL,   -- 'dimension' | 'fact'
        scd_type        VARCHAR(10) NOT NULL,   -- '0' | '1' | '2' | 'cdc'
        load_order      INT NOT NULL
    );
END
GO

IF OBJECT_ID('dbo.run_log') IS NULL
BEGIN
    CREATE TABLE dbo.run_log (
        run_log_id      INT IDENTITY(1,1) PRIMARY KEY,
        dag_id          VARCHAR(200) NULL,
        task_id         VARCHAR(200) NULL,
        started_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        finished_at     DATETIME2 NULL,
        status          VARCHAR(20) NULL,
        rows_processed  INT NULL,
        message         VARCHAR(MAX) NULL
    );
END
GO

MERGE dbo.table_config AS tgt
USING (VALUES
    ('DimDate',                 'dimension', '0',      1),
    ('DimGeography',            'dimension', '2',      2),
    ('DimSuppliers',            'dimension', '2',      3),
    ('DimProducts',             'dimension', '2',      4),
    ('DimCustomer',             'dimension', '2',      5),
    ('DimEmployees',            'dimension', '2',      6),
    ('DimTerritories',          'dimension', '2',      7),
    ('DimShippers',             'dimension', '1',      8),
    ('FactOrders',              'fact',      'cdc',    9),
    ('FactEmployeeTerritories', 'fact',      'bridge', 10)
) AS src (table_name, table_type, scd_type, load_order)
ON tgt.table_name = src.table_name
WHEN NOT MATCHED THEN
    INSERT (table_name, table_type, scd_type, load_order)
    VALUES (src.table_name, src.table_type, src.scd_type, src.load_order);
GO

-- ============ ENABLE CDC ON THE OLTP SOURCE ============

USE Northwind;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Northwind' AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_Orders')
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'Orders',
        @capture_instance     = N'dbo_Orders',
        @role_name            = NULL,
        @supports_net_changes = 1;
END
GO

IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_OrderDetails')
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'Order Details',
        @capture_instance     = N'dbo_OrderDetails',
        @role_name            = NULL,
        @supports_net_changes = 1;
END
GO

-- Phase 4 (scoped proof-of-concept): CDC on one dimension source table, so the
-- streaming consumer can soft-delete a dim row (is_current=0) when it's
-- removed from OLTP -- a gap the Phase 2 batch SCD jobs don't cover (they only
-- ever notice new/changed business keys, never one that vanished from source).
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_Suppliers')
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'Suppliers',
        @capture_instance     = N'dbo_Suppliers',
        @role_name            = NULL,
        @supports_net_changes = 1;
END
GO

USE ETL_Settings;
GO

MERGE dbo.cdc_state AS tgt
USING (VALUES
    ('Orders',         'dbo_Orders'),
    ('Order Details',  'dbo_OrderDetails'),
    ('Suppliers',      'dbo_Suppliers')
) AS src (source_table, capture_instance)
ON tgt.source_table = src.source_table
WHEN NOT MATCHED THEN
    INSERT (source_table, capture_instance, last_lsn, last_processed_at)
    VALUES (src.source_table, src.capture_instance, NULL, NULL);
GO
