# Quarantine: malformed records from every silver entity, with the rules they failed and the raw record,
# kept for the data engineering team to analyse. Unions the per-entity `bakehouse2_<e>_rejected` views.
from functools import reduce

from pyspark import pipelines as dp

from utilities.silver import ENTITIES

catalog = spark.conf.get("catalog")


@dp.table(
    name=f"{catalog}.silver.bakehouse2_quarantine",
    comment="Records rejected from silver, with the DQ rules they failed and the raw record",
    table_properties={"quality": "quarantine"},
)
def bakehouse2_quarantine():
    streams = [spark.readStream.table(f"bakehouse2_{e}_rejected") for e in ENTITIES]
    return reduce(lambda a, b: a.unionByName(b), streams)
