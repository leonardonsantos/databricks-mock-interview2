# databricks-mock-interview2

## bakehouse2 — bakery franchise medallion pipeline

Lakeflow **Spark Declarative Pipelines** (`from pyspark import pipelines as dp`) over `samples.bakehouse`
(franchise shops worldwide; each shop sources its main ingredient from one supplier). Deployed as a
Declarative Automation Bundle. Re-implements the existing `bakehouse_pipeline` under the `bakehouse2` prefix.

```
samples.bakehouse ──► bronze (streaming tables, raw + lineage)
                  ──► silver (typed/normalized, DQ-checked, Auto CDC SCD1 dedup)  ──► gold (materialized views)
                        └─► silver.bakehouse2_quarantine (rows failing any rule)
```

| Layer | Tables (`workspace.<layer>.`) | Type |
|---|---|---|
| Bronze | `bakehouse2_{suppliers,franchises,customers,transactions}_raw` | streaming table (as-is + `_source_table`, `_ingested_at`) |
| Silver | `bakehouse2_{suppliers,franchises,customers,transactions}` | streaming table, Auto CDC SCD1 on the business key |
| Silver | `bakehouse2_quarantine` | streaming table: `entity`, `record_key`, `dq_failed_rules`, `raw_record`, … |
| Gold | `bakehouse2_franchise_{daily,performance}`, `bakehouse2_product_{daily,performance}`, `bakehouse2_payment_daily`, `bakehouse2_supplier_performance` | materialized views |
| Gold | `bakehouse2_dq_entities`, `bakehouse2_dq_rules` | DQ monitoring MVs |

### Silver data quality
- **Types**: explicit casts; failed casts become nulls and are caught by not-null rules.
- **Normalization**: trimmed strings, blank → null, upper/lower-cased codes, country aliases, derived `region`.
- **Rules** (see `transformations/silver_*.py`): not-null keys/fields, value ranges (coordinates, quantity,
  timestamps), domains (payment method, size, gender, approved flag), email format, `total_price = quantity * unit_price`,
  foreign keys (transactions → franchises/customers). Rows failing any rule go to quarantine with the failed rule names
  and the raw record as JSON. `supplier_known` (franchises) is a warning only.
- Every rule is also a pipeline expectation, so pass/fail metrics land in the event log
  (`workspace.bronze.bakehouse2_pipeline_event_log`).
- **Dedup**: Auto CDC keeps one row per business key (latest ingestion wins); collapsed rows are reported as
  `duplicates_removed` in `gold.bakehouse2_dq_entities`.

### Layout
```
databricks.yml                          bundle, variables (catalog, source_schema), dev/prod targets
resources/bakehouse2.pipeline.yml       serverless pipeline definition
src/pipelines/bakehouse2/
  utilities/dq.py                       typing, normalization, rule helpers
  utilities/silver.py                   silver entity factory (checked/valid/rejected views + Auto CDC)
  transformations/bronze.py | silver_*.py | gold_*.py
```

### Deploy & run
```bash
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run bakehouse2_pipeline -t dev          # add --full-refresh-all to rebuild
```
