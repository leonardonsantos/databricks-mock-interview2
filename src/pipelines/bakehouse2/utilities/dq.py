"""Shared typing, normalization and data-quality helpers for the bakehouse2 silver layer."""
from pyspark.sql import Column
from pyspark.sql import functions as F

PAYMENT_METHODS = ("VISA", "MASTERCARD", "AMEX")
SIZES = ("S", "M", "L", "XL", "XXL")
EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
COUNTRY_ALIASES = {"US": "United States", "USA": "United States", "U.S.A.": "United States",
                   "UK": "United Kingdom"}
REGIONS = {"United States": "North America", "Canada": "North America", "Japan": "Asia", "Australia": "Oceania",
           "Netherlands": "Europe", "Germany": "Europe", "Sweden": "Europe", "Italy": "Europe", "France": "Europe"}


def sql_in(values) -> str:
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


def clean_str(c: str) -> Column:
    return F.nullif(F.trim(F.col(c).cast("string")), F.lit(""))


def country(c: str) -> Column:
    raw = F.upper(clean_str(c))
    mapped = F.lit(None).cast("string")
    for alias, name in COUNTRY_ALIASES.items():
        mapped = F.when(raw == alias, name).otherwise(mapped)
    return F.coalesce(mapped, F.initcap(clean_str(c)))


def region(country_col: str) -> Column:
    out = F.lit(None).cast("string")
    for ctry, reg in REGIONS.items():
        out = F.when(F.col(country_col) == ctry, reg).otherwise(out)
    return out


def guarded(rules: dict) -> dict:
    # NULL must fail a rule (expectations treat NULL as pass)
    return {name: f"coalesce({cond}, false)" for name, cond in rules.items()}


def failed_array(rules: dict) -> Column:
    items = [F.when(~F.expr(c), F.lit(n)) for n, c in guarded(rules).items()]
    if not items:
        return F.array().cast("array<string>")
    return F.array_compact(F.array(*items)).cast("array<string>")


def typed(bronze, typed_cols: dict):
    """Apply typing/normalization, keep the raw record (JSON) and lineage columns."""
    raw_cols = [c for c in bronze.columns if not c.startswith("_")]
    raw = F.to_json(F.struct(*raw_cols))
    return bronze.select(
        *[expr.alias(name) for name, expr in typed_cols.items()],
        raw.alias("_raw_record"),
        F.sha2(raw, 256).alias("_row_hash"),
        "_source_table",
        "_ingested_at",
    )


def flag(df, rules: dict, warn: dict):
    return df.withColumn("_dq_failed_rules", failed_array(rules)).withColumn("_dq_warnings", failed_array(warn))
