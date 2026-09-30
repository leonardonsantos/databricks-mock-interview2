# Bronze: incremental, append-only copy of the Databricks sample `samples.bakehouse` plus lineage columns.
# No typing/cleaning here — that happens in silver.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
source = spark.conf.get("source_schema")

ENTITIES = {
    "suppliers": "sales_suppliers",
    "franchises": "sales_franchises",
    "customers": "sales_customers",
    "transactions": "sales_transactions",
}


def bronze_table(entity, src):
    @dp.table(name=f"{catalog}.bronze.bakehouse2_{entity}_raw", comment=f"Raw copy of {source}.{src}",
              table_properties={"quality": "bronze"})
    def _bronze():
        # the sample tables are occasionally rewritten upstream; only appended rows are ingested
        return (spark.readStream.option("skipChangeCommits", "true").table(f"{source}.{src}")
                .withColumn("_source_table", F.lit(f"{source}.{src}"))
                .withColumn("_ingested_at", F.current_timestamp()))


for entity, src in ENTITIES.items():
    bronze_table(entity, src)
