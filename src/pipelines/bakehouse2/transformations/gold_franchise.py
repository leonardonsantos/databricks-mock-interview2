# Gold: franchise-level stakeholder views (materialized views, amounts in USD).
# samples.bakehouse covers ~17 days, so momentum is last 7 days vs the 7 days before.
from pyspark import pipelines as dp

c = spark.conf.get("catalog")


@dp.materialized_view(name=f"{c}.gold.bakehouse2_franchise_daily",
                      comment="Revenue, transactions and basket size per franchise and day",
                      table_properties={"quality": "gold"})
@dp.expect_or_fail("pk_not_null", "franchise_id IS NOT NULL AND sales_date IS NOT NULL")
@dp.expect("non_negative_revenue", "revenue >= 0")
def bakehouse2_franchise_daily():
    return spark.sql(f"""
        SELECT t.franchise_id, t.txn_date AS sales_date,
               f.franchise_name, f.city, f.country, f.region, f.size, f.supplier_id,
               sum(t.total_price) AS revenue,
               count(DISTINCT t.transaction_id) AS transactions,
               sum(t.quantity) AS items_sold,
               count(DISTINCT t.customer_id) AS customers,
               round(sum(t.total_price) / count(DISTINCT t.transaction_id), 2) AS avg_basket
        FROM {c}.silver.bakehouse2_transactions t
        JOIN {c}.silver.bakehouse2_franchises f USING (franchise_id)
        GROUP BY ALL""")


@dp.materialized_view(name=f"{c}.gold.bakehouse2_franchise_performance",
                      comment="Franchise league table: totals, rank, 7-day growth, supplier",
                      table_properties={"quality": "gold"})
@dp.expect_or_fail("pk_not_null", "franchise_id IS NOT NULL")
def bakehouse2_franchise_performance():
    return spark.sql(f"""
        WITH bounds AS (SELECT max(sales_date) AS as_of FROM {c}.gold.bakehouse2_franchise_daily),
        agg AS (
          SELECT d.franchise_id,
            sum(d.revenue) AS revenue, sum(d.transactions) AS transactions, sum(d.items_sold) AS items_sold,
            sum(CASE WHEN d.sales_date > b.as_of - 7 THEN d.revenue ELSE 0 END) AS revenue_last_7d,
            sum(CASE WHEN d.sales_date <= b.as_of - 7 AND d.sales_date > b.as_of - 14
                     THEN d.revenue ELSE 0 END) AS revenue_prev_7d,
            count(DISTINCT d.sales_date) AS trading_days,
            max(b.as_of) AS as_of
          FROM {c}.gold.bakehouse2_franchise_daily d CROSS JOIN bounds b
          GROUP BY d.franchise_id
        )
        SELECT f.franchise_id, f.franchise_name, f.city, f.district, f.country, f.region, f.size,
          f.supplier_id, coalesce(s.supplier_name, 'Unknown supplier') AS supplier_name,
          s.ingredient AS supplier_ingredient, s.supplier_id IS NOT NULL AS supplier_known,
          a.revenue, a.transactions, a.items_sold,
          round(a.revenue / a.transactions, 2) AS avg_basket,
          round(a.revenue / a.trading_days, 2) AS avg_daily_revenue,
          a.revenue_last_7d, a.revenue_prev_7d,
          round(100 * (a.revenue_last_7d - a.revenue_prev_7d) / nullif(a.revenue_prev_7d, 0), 1) AS growth_pct,
          rank() OVER (ORDER BY a.revenue DESC) AS revenue_rank,
          CASE WHEN rank() OVER (ORDER BY a.revenue DESC) <= 10 THEN 'Top 10'
               WHEN rank() OVER (ORDER BY a.revenue ASC) <= 10 THEN 'Bottom 10'
               ELSE 'Middle' END AS performance_band,
          a.as_of
        FROM agg a
        JOIN {c}.silver.bakehouse2_franchises f USING (franchise_id)
        LEFT JOIN {c}.silver.bakehouse2_suppliers s ON f.supplier_id = s.supplier_id""")
