"""
Reusable PySpark helpers for JDBC I/O against Postgres staging / ClickHouse
NorthwindDW, and for SCD1/SCD2 dimension merge logic.

Usage from a job script:

    from pyspark.sql import SparkSession
    import scd_lib

    spark = SparkSession.builder.appName("load_dim_x").getOrCreate()
    scd_lib.init(
        spark,
        pg_jdbc_url=args.pg_jdbc_url, pg_user=args.pg_user, pg_password=args.pg_password,
        ch_jdbc_url=args.ch_jdbc_url, ch_user=args.ch_user, ch_password=args.ch_password,
    )
    df = scd_lib.read_pg("staging.stg_suppliers")
    ...

Conventions this module relies on (true for every dim/fact in this project):
  - every dimension table's surrogate key is its FIRST column.
  - SCD2 dims have is_current / Startdate / Enddate / version columns;
    SCD1 dims have only a version column (no history tracking).
  - all tables use ReplacingMergeTree(version); reads always go through
    `... FINAL` so a table's true collapsed state is seen.
"""
import argparse
import time

from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, DateType
from pyspark.sql.window import Window

PG_DRIVER = "org.postgresql.Driver"
CH_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

FAR_FUTURE_DATE = "2099-12-31"  # ClickHouse's 2-byte Date type maxes out around 2149-06-06
_NULL_MARKER = "__NULL__"

_state = {}


def init(spark, pg_jdbc_url, pg_user, pg_password, ch_jdbc_url, ch_user, ch_password,
         mssql_jdbc_url=None, mssql_user=None, mssql_password=None):
    """Call once per job run before using any other helper in this module."""
    _state["spark"] = spark
    _state["pg"] = {"url": pg_jdbc_url, "user": pg_user, "password": pg_password, "driver": PG_DRIVER}
    _state["ch"] = {"url": ch_jdbc_url, "user": ch_user, "password": ch_password, "driver": CH_DRIVER}
    if mssql_jdbc_url is not None:
        _state["mssql"] = {"url": mssql_jdbc_url, "user": mssql_user, "password": mssql_password, "driver": MSSQL_DRIVER}
    _state["run_version"] = int(time.time() * 1000)


def build_arg_parser(description, needs_mssql=False):
    """Standard JDBC connection args shared by every dimension/fact load job."""
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--pg-jdbc-url", required=True)
    p.add_argument("--pg-user", required=True)
    p.add_argument("--pg-password", required=True)
    p.add_argument("--ch-jdbc-url", required=True)
    p.add_argument("--ch-user", required=True)
    p.add_argument("--ch-password", required=True)
    if needs_mssql:
        p.add_argument("--mssql-jdbc-url", required=True)
        p.add_argument("--mssql-user", required=True)
        p.add_argument("--mssql-password", required=True)
    return p


def init_from_args(spark, args):
    init(
        spark,
        pg_jdbc_url=args.pg_jdbc_url, pg_user=args.pg_user, pg_password=args.pg_password,
        ch_jdbc_url=args.ch_jdbc_url, ch_user=args.ch_user, ch_password=args.ch_password,
        mssql_jdbc_url=getattr(args, "mssql_jdbc_url", None),
        mssql_user=getattr(args, "mssql_user", None),
        mssql_password=getattr(args, "mssql_password", None),
    )


def run_version():
    """A single strictly-increasing version number shared by every write in this job run."""
    return _state["run_version"]


def _jdbc_read(opts, dbtable):
    spark = _state["spark"]
    return (
        spark.read.format("jdbc")
        .option("url", opts["url"])
        .option("dbtable", dbtable)
        .option("user", opts["user"])
        .option("password", opts["password"])
        .option("driver", opts["driver"])
        .load()
    )


def read_pg(table):
    return _jdbc_read(_state["pg"], table)


def read_mssql(query_or_table):
    return _jdbc_read(_state["mssql"], query_or_table)


def read_ch(table):
    """Reads a ClickHouse table collapsed with FINAL, so ReplacingMergeTree's true
    current state is seen regardless of background merge state."""
    return _jdbc_read(_state["ch"], f"(SELECT * FROM {table} FINAL) t")


def materialize(df):
    """Caches df and forces one action so its result is frozen. Must be called on any
    DataFrame that both feeds write_ch_append AND is inspected afterwards (e.g. via
    .count() for logging) -- without this, the second action re-executes the whole
    lazy plan, which re-reads ClickHouse's now-already-written state and silently
    produces wrong "nothing to do" results."""
    df = df.cache()
    df.count()
    return df


def write_ch_append(df, table):
    opts = _state["ch"]
    (
        df.write.format("jdbc")
        .option("url", opts["url"])
        .option("dbtable", table)
        .option("user", opts["user"])
        .option("password", opts["password"])
        .option("driver", opts["driver"])
        .mode("append")
        .save()
    )


def assign_surrogate_keys(df, dim_table, key_col):
    """Assigns brand-new surrogate keys to every row in df, continuing from the current
    max key in ClickHouse -- existing keys already in the table are never touched or
    renumbered."""
    current_max = (
        read_ch(dim_table)
        .agg(F.coalesce(F.max(F.col(key_col)), F.lit(0)).cast("long").alias("max_key"))
        .collect()[0]["max_key"]
    )
    # Small (Northwind-scale) data: a single-partition row_number is fine.
    w = Window.orderBy(F.monotonically_increasing_id())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .withColumn(key_col, (F.lit(current_max).cast("long") + F.col("_rn")).cast(LongType()))
        .drop("_rn")
    )


def _hash_attrs(cols):
    return F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit(_NULL_MARKER)) for c in cols]),
        256,
    )


def scd2_apply(source_df, dim_table, key_col, business_key, scd_attrs):
    """
    source_df must contain exactly business_key + every non-managed column the target
    table needs (scd_attrs plus any resolved FK / passthrough columns) -- NOT key_col,
    is_current, Startdate, Enddate or version; those are managed here.

    - business keys with no current dim row  -> insert, new surrogate key, is_current=1
    - business keys whose scd_attrs changed   -> expire old row (same key, is_current=0,
                                                  Enddate=today, bumped version) AND
                                                  insert a new row (new surrogate key,
                                                  is_current=1, Startdate=today)
    - business keys whose scd_attrs match     -> no write

    Returns the DataFrame of rows written (may be empty), already appended to ClickHouse.
    """
    version = run_version()
    other_cols = [c for c in source_df.columns if c != business_key]

    current = read_ch(dim_table).filter(F.col("is_current") == 1)

    src = source_df.withColumn("_hash", _hash_attrs(scd_attrs))
    cur = current.withColumn("_hash", _hash_attrs(scd_attrs))

    joined = src.alias("s").join(cur.alias("c"), on=business_key, how="left")

    is_new = F.col(f"c.{key_col}").isNull()
    is_changed = F.col(f"c.{key_col}").isNotNull() & (F.col("s._hash") != F.col("c._hash"))

    def select_from(prefix, extra_cols=None):
        cols = [F.col(business_key).alias(business_key)]
        cols += [F.col(f"{prefix}.{c}").alias(c) for c in other_cols]
        return cols + (extra_cols or [])

    new_rows = joined.filter(is_new).select(*select_from("s"))
    changed_new_version = joined.filter(is_changed).select(*select_from("s"))

    to_insert = new_rows.unionByName(changed_new_version)
    inserted = (
        assign_surrogate_keys(to_insert, dim_table, key_col)
        .withColumn("is_current", F.lit(1).cast("int"))
        .withColumn("Startdate", F.current_date())
        .withColumn("Enddate", F.to_date(F.lit(FAR_FUTURE_DATE)))
        .withColumn("version", F.lit(version).cast(LongType()))
    )

    expired = (
        joined.filter(is_changed)
        .select(*select_from("c", extra_cols=[F.col(f"c.{key_col}").alias(key_col)]))
        .withColumn("is_current", F.lit(0).cast("int"))
        .withColumn("Enddate", F.current_date())
        .withColumn("version", F.lit(version).cast(LongType()))
    )
    # Startdate carried over unchanged from the current (now-expiring) row.
    expired = expired.join(
        current.select(F.col(key_col).alias(f"_orig_{key_col}"), F.col("Startdate").alias("_orig_start")),
        on=F.col(key_col) == F.col(f"_orig_{key_col}"),
        how="left",
    ).drop(f"_orig_{key_col}", "Startdate").withColumnRenamed("_orig_start", "Startdate")

    to_write = materialize(inserted.unionByName(expired.select(inserted.columns)))
    write_ch_append(to_write, dim_table)
    return to_write


def scd1_apply(source_df, dim_table, key_col, business_key):
    """
    source_df must contain business_key + all non-managed attribute columns matching
    the target table (everything except key_col/version).

    - new business keys      -> insert with a new surrogate key
    - changed attribute rows -> same surrogate key, bumped version (ReplacingMergeTree
                                 keeps only the latest version per key on FINAL reads)
    - unchanged rows         -> no write

    Returns the DataFrame of rows written (may be empty), already appended to ClickHouse.
    """
    version = run_version()
    attrs = [c for c in source_df.columns if c != business_key]

    current = read_ch(dim_table)
    src = source_df.withColumn("_hash", _hash_attrs(attrs))
    cur = current.withColumn("_hash", _hash_attrs(attrs))

    joined = src.alias("s").join(cur.alias("c"), on=business_key, how="left")

    def select_from(prefix, extra_cols=None):
        cols = [F.col(business_key).alias(business_key)]
        cols += [F.col(f"{prefix}.{c}").alias(c) for c in attrs]
        return cols + (extra_cols or [])

    new_rows = joined.filter(F.col(f"c.{key_col}").isNull()).select(*select_from("s"))
    changed_rows = (
        joined.filter(F.col(f"c.{key_col}").isNotNull() & (F.col("s._hash") != F.col("c._hash")))
        .select(*select_from("s", extra_cols=[F.col(f"c.{key_col}").alias(key_col)]))
        .withColumn("version", F.lit(version).cast(LongType()))
    )

    new_with_keys = assign_surrogate_keys(new_rows, dim_table, key_col).withColumn(
        "version", F.lit(version).cast(LongType())
    )

    to_write = materialize(new_with_keys.unionByName(changed_rows))
    write_ch_append(to_write, dim_table)
    return to_write


def lookup(df, dim_table, on, key_out):
    """
    Resolves a foreign surrogate key by joining df to the CURRENT (is_current=1) rows
    of dim_table (dims with no is_current column, e.g. SCD1/static dims, are used as-is).
    The surrogate key column is assumed to be dim_table's first column (true by
    convention for every dim in this project).

    `on` may be:
      - a column name (str) shared by df and dim_table, or
      - a list of shared column names (multi-column join), or
      - a dict {df_column: dim_column} for differing names.

    Join is NULL-safe (Northwind addresses routinely have NULL Region), so rows with
    NULL join columns on both sides still match. Returns df with `key_out` appended
    (NULL if no match is found).
    """
    dim = read_ch(dim_table)
    if "is_current" in dim.columns:
        dim = dim.filter(F.col("is_current") == 1)
    key_col = dim.columns[0]

    if isinstance(on, str):
        pairs = [(on, on)]
    elif isinstance(on, dict):
        pairs = list(on.items())
    else:
        pairs = [(c, c) for c in on]

    dim_renamed = dim.select(
        F.col(key_col).alias(key_out),
        *[F.col(dc).alias(f"_lk_{i}") for i, (_, dc) in enumerate(pairs)],
    )

    cond = None
    for i, (sc, _) in enumerate(pairs):
        c = df[sc].eqNullSafe(dim_renamed[f"_lk_{i}"])
        cond = c if cond is None else (cond & c)

    df = df.drop(key_out)
    joined = df.join(dim_renamed, on=cond, how="left").drop(*[f"_lk_{i}" for i in range(len(pairs))])
    return joined


def _ch_column_nullability(table):
    """True nullability per column, straight from ClickHouse's own system.columns.
    Spark's DataFrame.schema[*].nullable (as reported through the JDBC driver) is
    NOT reliable for ClickHouse -- it comes back True for columns that are actually
    non-nullable in the table definition, which fails at write time with
    'Cannot set null to non-nullable column'."""
    db, _, tbl = table.rpartition(".")
    rows = _jdbc_read(
        _state["ch"], f"(SELECT name, type FROM system.columns WHERE database = '{db}' AND table = '{tbl}') t"
    ).collect()
    return {r["name"]: r["type"].startswith("Nullable(") for r in rows}


def _on_pairs(on):
    if isinstance(on, str):
        return [(on, on)]
    if isinstance(on, dict):
        return list(on.items())
    return [(c, c) for c in on]


def lookup_or_infer(df, dim_table, on, key_out):
    """
    Like lookup(), but any row whose natural key doesn't match a current dim row
    gets a minimal placeholder inserted into dim_table first (business/natural key
    columns filled in, everything else defaulted -- 'Unknown' for non-nullable
    strings, 0 for non-nullable numerics, NULL for nullable columns -- is_current=1,
    is_inferred=1, a fresh stable surrogate key), and the lookup is re-run so it
    resolves to that placeholder's key.

    Used for CDC fact loads, where a change can reference a business key
    (ProductID, CustomerID, ...) that hasn't reached its dimension yet. Filling in
    the *real* attributes for an inferred row later is a separate reconciliation
    step (not implemented here) -- see docs/phase3_inferred_members.md.
    """
    resolved = lookup(df, dim_table, on, key_out)
    if not resolved.filter(F.col(key_out).isNull()).take(1):
        return resolved

    pairs = _on_pairs(on)
    natural_cols = {dc for _, dc in pairs}

    dim_current = read_ch(dim_table)
    dim_key_col = dim_current.columns[0]

    missing_keys = (
        resolved.filter(F.col(key_out).isNull())
        .select(*[F.col(sc).alias(dc) for sc, dc in pairs])
        .distinct()
    )

    true_nullable = _ch_column_nullability(dim_table)

    placeholder = missing_keys
    for field in dim_current.schema.fields:
        name = field.name
        if name in natural_cols or name == dim_key_col:
            continue
        if name == "is_current":
            placeholder = placeholder.withColumn(name, F.lit(1).cast(field.dataType))
        elif name == "Startdate":
            placeholder = placeholder.withColumn(name, F.current_date())
        elif name == "Enddate":
            placeholder = placeholder.withColumn(name, F.to_date(F.lit(FAR_FUTURE_DATE)))
        elif name == "version":
            placeholder = placeholder.withColumn(name, F.lit(run_version()).cast(LongType()))
        elif name == "is_inferred":
            placeholder = placeholder.withColumn(name, F.lit(1).cast(field.dataType))
        elif true_nullable.get(name, field.nullable):
            placeholder = placeholder.withColumn(name, F.lit(None).cast(field.dataType))
        elif isinstance(field.dataType, StringType):
            placeholder = placeholder.withColumn(name, F.lit("Unknown").cast(field.dataType))
        elif isinstance(field.dataType, (DateType,)):
            placeholder = placeholder.withColumn(name, F.current_date())
        else:
            placeholder = placeholder.withColumn(name, F.lit(0).cast(field.dataType))

    placeholder = assign_surrogate_keys(placeholder, dim_table, dim_key_col)
    placeholder = materialize(placeholder.select(*dim_current.columns))
    write_ch_append(placeholder, dim_table)

    return lookup(df, dim_table, on, key_out)
