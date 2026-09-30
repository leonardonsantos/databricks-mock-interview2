# Gold: product-level stakeholder views.
from pyspark import pipelines as dp

c = spark.conf.get("catalog")


@dp.materialized_view(name=f"{c}.gold.bakehouse2_product_daily",
                      comment="Units and revenue per product, day and region",
                      table_properties={"quality": "gold"})
def bakehouse2_product_daily():
    return spark.sql(f"""
        SELECT t.product, t.txn_date AS sales_date, f.region,
               sum(t.quantity) AS units_sold,
               sum(t.total_price) AS revenue,
               count(DISTINCT t.transaction_id) AS transactions
        FROM {c}.silver.bakehouse2_transactions t
        JOIN {c}.silver.bakehouse2_franchises f USING (franchise_id)
        GROUP BY ALL""")


@dp.materialized_view(name=f"{c}.gold.bakehouse2_product_performance",
                      comment="Product league table: units, revenue share, rank, 7-day growth, best region",
                      table_properties={"quality": "gold"})
def bakehouse2_product_performance():
    return spark.sql(f"""
        WITH bounds AS (SELECT max(sales_date) AS as_of FROM {c}.gold.bakehouse2_product_daily),
        by_region AS (
          SELECT product, region, sum(revenue) AS revenue,
                 row_number() OVER (PARTITION BY product ORDER BY sum(revenue) DESC) AS rn
          FROM {c}.gold.bakehouse2_product_daily GROUP BY product, region
        ),
        agg AS (
          SELECT d.product,
            sum(d.units_sold) AS units_sold, sum(d.revenue) AS revenue, sum(d.transactions) AS transactions,
            sum(CASE WHEN d.sales_date > b.as_of - 7 THEN d.revenue ELSE 0 END) AS revenue_last_7d,
            sum(CASE WHEN d.sales_date <= b.as_of - 7 AND d.sales_date > b.as_of - 14
                     THEN d.revenue ELSE 0 END) AS revenue_prev_7d
          FROM {c}.gold.bakehouse2_product_daily d CROSS JOIN bounds b
          GROUP BY ALL
        ),
        prices AS (SELECT product, max(unit_price) AS list_price
                   FROM {c}.silver.bakehouse2_transactions GROUP BY product)
        SELECT a.product, p.list_price, a.units_sold, a.revenue, a.transactions,
          round(100 * a.revenue / sum(a.revenue) OVER (), 2) AS revenue_share_pct,
          rank() OVER (ORDER BY a.revenue DESC) AS revenue_rank,
          round(100 * (a.revenue_last_7d - a.revenue_prev_7d) / nullif(a.revenue_prev_7d, 0), 1) AS growth_pct,
          r.region AS best_region
        FROM agg a
        JOIN prices p USING (product)
        JOIN by_region r ON r.product = a.product AND r.rn = 1""")
