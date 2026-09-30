from pyspark.sql import functions as F

from utilities.dq import SIZES, clean_str, country, region, sql_in
from utilities.silver import define_silver_entity


def _enrich(spark, catalog, df):
    sup = spark.read.table(f"{catalog}.silver.bakehouse2_suppliers").select(
        "supplier_id", F.lit(True).alias("_supplier_known"))
    return df.join(F.broadcast(sup), "supplier_id", "left").withColumn("region", region("country"))


define_silver_entity(
    spark, "franchises", "franchise_id",
    typed_cols={
        "franchise_id": F.col("franchiseID").cast("bigint"), "franchise_name": clean_str("name"),
        "city": clean_str("city"), "district": clean_str("district"), "zipcode": clean_str("zipcode"),
        "country": country("country"), "size": F.upper(clean_str("size")),
        "longitude": F.col("longitude").cast("double"), "latitude": F.col("latitude").cast("double"),
        "supplier_id": F.col("supplierID").cast("bigint"),
    },
    rules={
        "franchise_id_not_null": "franchise_id IS NOT NULL",
        "franchise_name_not_null": "franchise_name IS NOT NULL",
        "country_not_null": "country IS NOT NULL",
        "size_valid": f"size IN {sql_in(SIZES)}",
        "coordinates_in_range": "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180",
    },
    # many sample franchises reference a supplierID missing from sales_suppliers: report, don't drop the shop
    warn={"supplier_known": "supplier_id IS NOT NULL AND _supplier_known"},
    enrich=_enrich, extra_cols=["region"],
    comment="Franchise shops with region; supplier may be unknown (see _dq_warnings)",
)
