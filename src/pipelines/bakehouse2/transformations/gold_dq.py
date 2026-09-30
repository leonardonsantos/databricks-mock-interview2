# Gold: data-quality reporting for stakeholders and the data engineering team.
from pyspark import pipelines as dp

c = spark.conf.get("catalog")
ENTITIES = ("suppliers", "franchises", "customers", "transactions")


def _union(template):
    return "\nUNION ALL ".join(template.format(e=e) for e in ENTITIES)


@dp.materialized_view(name=f"{c}.gold.bakehouse2_dq_rules",
                      comment="Records failing each DQ rule, per entity and severity "
                              "(quarantine = row rejected, warning = row kept and flagged)",
                      table_properties={"quality": "gold"})
def bakehouse2_dq_rules():
    warnings = _union(f"SELECT '{{e}}' AS entity, _dq_warnings FROM {c}.silver.bakehouse2_{{e}}")
    return spark.sql(f"""
        SELECT entity, rule, 'quarantine' AS severity, count(*) AS failed_records
        FROM {c}.silver.bakehouse2_quarantine LATERAL VIEW explode(dq_failed_rules) r AS rule
        GROUP BY entity, rule
        UNION ALL
        SELECT entity, rule, 'warning' AS severity, count(*) AS failed_records
        FROM ({warnings}) LATERAL VIEW explode(_dq_warnings) r AS rule
        GROUP BY entity, rule""")


@dp.materialized_view(name=f"{c}.gold.bakehouse2_dq_entities",
                      comment="Record counts per layer; bronze = silver + quarantined + duplicates_removed",
                      table_properties={"quality": "gold"})
@dp.expect("reconciles", "duplicates_removed >= 0")
def bakehouse2_dq_entities():
    bronze = _union(f"SELECT '{{e}}' AS entity, count(*) AS n FROM {c}.bronze.bakehouse2_{{e}}_raw")
    silver = _union(f"SELECT '{{e}}' AS entity, count(*) AS n, count_if(size(_dq_warnings) > 0) AS w "
                    f"FROM {c}.silver.bakehouse2_{{e}}")
    return spark.sql(f"""
        WITH bronze AS ({bronze}), silver AS ({silver}),
        q AS (SELECT entity, count(*) AS n FROM {c}.silver.bakehouse2_quarantine GROUP BY entity)
        SELECT b.entity, b.n AS bronze_records, s.n AS silver_records,
          coalesce(q.n, 0) AS quarantined_records,
          b.n - s.n - coalesce(q.n, 0) AS duplicates_removed,
          s.w AS warned_records,
          round(100 * coalesce(q.n, 0) / b.n, 2) AS quarantine_pct
        FROM bronze b JOIN silver s USING (entity) LEFT JOIN q USING (entity)""")
