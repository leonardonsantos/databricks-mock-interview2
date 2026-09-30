# Gold: one row per supplier (+ an 'unknown' bucket): does sourcing relate to franchise performance?
from pyspark import pipelines as dp

c = spark.conf.get("catalog")


@dp.materialized_view(name=f"{c}.gold.bakehouse2_supplier_performance",
                      comment="Suppliers: franchises supplied and the revenue they generate (-1 = unknown supplier)",
                      table_properties={"quality": "gold"})
def bakehouse2_supplier_performance():
    return spark.sql(f"""
        SELECT coalesce(sup.supplier_id, -1) AS supplier_id,
          coalesce(sup.supplier_name, 'Unknown supplier') AS supplier_name,
          sup.ingredient, sup.continent, sup.approved,
          count(p.franchise_id) AS franchises_supplied,
          coalesce(sum(p.revenue), 0) AS revenue,
          round(avg(p.revenue), 2) AS avg_revenue_per_franchise,
          round(avg(p.avg_daily_revenue), 2) AS avg_daily_revenue_per_franchise
        FROM {c}.silver.bakehouse2_suppliers sup
        FULL OUTER JOIN {c}.gold.bakehouse2_franchise_performance p ON p.supplier_id = sup.supplier_id
        GROUP BY ALL""")
