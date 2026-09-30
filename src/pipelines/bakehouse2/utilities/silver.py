"""Factory for one silver entity:

  bakehouse2_<e>_checked   temporary view (streaming): typed + normalized + `_dq_failed_rules` / `_dq_warnings`;
                           every rule is an expectation, so pass/fail counts land in the event log
  bakehouse2_<e>_valid     temporary view: rows with no failed rule
  <catalog>.silver.bakehouse2_<e>
                           streaming table maintained by Auto CDC (SCD1) on the business key -> one row per key
                           (duplicates collapse, latest ingestion wins)
  bakehouse2_<e>_rejected  temporary view: rows with >= 1 failed rule (unioned into silver.bakehouse2_quarantine)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.dq import flag, guarded, typed

ENTITIES = ("suppliers", "franchises", "customers", "transactions")

def define_silver_entity(spark, name, key, typed_cols, rules, warn=None, enrich=None, extra_cols=(), comment=""):
    catalog = spark.conf.get("catalog")
    warn = warn or {}
    checked, valid = f"bakehouse2_{name}_checked", f"bakehouse2_{name}_valid"
    target = f"{catalog}.silver.bakehouse2_{name}"

    @dp.temporary_view(name=checked, comment=f"{name}: typed, normalized, DQ-flagged")
    @dp.expect_all(guarded({**rules, **warn}))
    def _checked():
        df = typed(spark.readStream.table(f"{catalog}.bronze.bakehouse2_{name}_raw"), typed_cols)
        if enrich:
            df = enrich(spark, catalog, df)
        return flag(df, rules, warn)

    @dp.temporary_view(name=valid, comment=f"{name}: rows passing all quarantine rules")
    def _valid():
        return (spark.readStream.table(checked)
                .where(F.size("_dq_failed_rules") == 0)
                .select(*typed_cols.keys(), *extra_cols, "_dq_warnings", "_row_hash", "_ingested_at"))

    dp.create_streaming_table(name=target, comment=comment, table_properties={"quality": "silver"})
    dp.create_auto_cdc_flow(
        target=target,
        source=valid,
        keys=[key],
        sequence_by=F.struct("_ingested_at", "_row_hash"),
        stored_as_scd_type=1,
        name=f"bakehouse2_{name}_upsert",
    )

    @dp.temporary_view(name=f"bakehouse2_{name}_rejected", comment=f"{name}: rows failing >= 1 quarantine rule")
    def _rejected():
        return (spark.readStream.table(checked)
                .where(F.size("_dq_failed_rules") > 0)
                .select(F.lit(name).alias("entity"), F.col(key).cast("string").alias("record_key"),
                        F.col("_dq_failed_rules").alias("dq_failed_rules"), F.col("_raw_record").alias("raw_record"),
                        "_source_table", "_ingested_at", F.current_timestamp().alias("quarantined_at")))
