from pyspark.sql import functions as F

from utilities.dq import PAYMENT_METHODS, clean_str, sql_in
from utilities.silver import define_silver_entity


def _enrich(spark, catalog, df):
    shops = spark.read.table(f"{catalog}.silver.bakehouse2_franchises").select(
        "franchise_id", F.lit(True).alias("_franchise_known"))
    custs = spark.read.table(f"{catalog}.silver.bakehouse2_customers").select(
        "customer_id", F.lit(True).alias("_customer_known"))
    return (df.join(F.broadcast(shops), "franchise_id", "left").join(F.broadcast(custs), "customer_id", "left")
            .withColumn("txn_date", F.to_date("txn_ts")))


define_silver_entity(
    spark, "transactions", "transaction_id",
    typed_cols={
        "transaction_id": F.col("transactionID").cast("bigint"), "customer_id": F.col("customerID").cast("bigint"),
        "franchise_id": F.col("franchiseID").cast("bigint"), "txn_ts": F.col("dateTime").cast("timestamp"),
        "product": clean_str("product"), "quantity": F.col("quantity").cast("int"),
        "unit_price": F.col("unitPrice").cast("decimal(10,2)"),
        "total_price": F.col("totalPrice").cast("decimal(12,2)"),
        "payment_method": F.upper(clean_str("paymentMethod")),
        "card_last4": F.right(F.col("cardNumber").cast("string"), F.lit(4)),  # never keep the full PAN
    },
    rules={
        "transaction_id_not_null": "transaction_id IS NOT NULL",
        "franchise_id_not_null": "franchise_id IS NOT NULL",
        "franchise_known": "franchise_id IS NULL OR _franchise_known",
        "customer_known": "customer_id IS NULL OR _customer_known",
        "product_not_null": "product IS NOT NULL",
        "quantity_in_range": "quantity BETWEEN 1 AND 100",
        "unit_price_positive": "unit_price > 0",
        "total_price_matches": "total_price = quantity * unit_price",
        "txn_ts_in_range": "txn_ts >= timestamp'2020-01-01' AND txn_ts <= current_timestamp() + INTERVAL 1 DAY",
        "payment_method_valid": f"payment_method IN {sql_in(PAYMENT_METHODS)}",
    },
    enrich=_enrich, extra_cols=["txn_date"],
    comment="POS transactions (one product per transaction), USD",
)
