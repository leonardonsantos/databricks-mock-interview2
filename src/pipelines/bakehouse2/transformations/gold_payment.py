# Gold: payment method (card brand) x country x day.
from pyspark import pipelines as dp

from utilities.dq import PAYMENT_METHODS, sql_in

c = spark.conf.get("catalog")


@dp.materialized_view(name=f"{c}.gold.bakehouse2_payment_daily",
                      comment="Transactions and revenue per card brand, country and day",
                      table_properties={"quality": "gold"})
@dp.expect("known_method", f"payment_method IN {sql_in(PAYMENT_METHODS)}")
def bakehouse2_payment_daily():
    return spark.sql(f"""
        SELECT t.txn_date AS sales_date, f.country, t.payment_method, f.region,
               count(DISTINCT t.transaction_id) AS transactions,
               sum(t.total_price) AS revenue
        FROM {c}.silver.bakehouse2_transactions t
        JOIN {c}.silver.bakehouse2_franchises f USING (franchise_id)
        GROUP BY ALL""")
