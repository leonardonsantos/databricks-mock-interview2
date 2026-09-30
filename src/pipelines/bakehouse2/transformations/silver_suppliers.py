from pyspark.sql import functions as F

from utilities.dq import clean_str
from utilities.silver import define_silver_entity

define_silver_entity(
    spark, "suppliers", "supplier_id",
    typed_cols={
        "supplier_id": F.col("supplierID").cast("bigint"), "supplier_name": clean_str("name"),
        "ingredient": F.lower(clean_str("ingredient")), "continent": clean_str("continent"),
        "city": clean_str("city"), "district": clean_str("district"), "size": F.upper(clean_str("size")),
        "approved": F.upper(clean_str("approved")),
        "longitude": F.col("longitude").cast("double"), "latitude": F.col("latitude").cast("double"),
    },
    rules={
        "supplier_id_not_null": "supplier_id IS NOT NULL",
        "supplier_name_not_null": "supplier_name IS NOT NULL",
        "ingredient_not_null": "ingredient IS NOT NULL",
        "approved_valid": "approved IN ('Y', 'N')",
        "coordinates_in_range": "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180",
    },
    comment="Ingredient suppliers (each franchise sources its main ingredient from one supplier)",
)
