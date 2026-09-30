from pyspark.sql import functions as F

from utilities.dq import EMAIL_RE, clean_str, country
from utilities.silver import define_silver_entity

define_silver_entity(
    spark, "customers", "customer_id",
    typed_cols={
        "customer_id": F.col("customerID").cast("bigint"), "first_name": F.initcap(clean_str("first_name")),
        "last_name": F.initcap(clean_str("last_name")), "email": F.lower(clean_str("email_address")),
        "phone_number": clean_str("phone_number"), "city": clean_str("city"), "state": clean_str("state"),
        "country": country("country"), "continent": clean_str("continent"),
        "postal_zip_code": clean_str("postal_zip_code"), "gender": F.lower(clean_str("gender")),
    },
    rules={
        "customer_id_not_null": "customer_id IS NOT NULL",
        "email_valid": f"email RLIKE '{EMAIL_RE}'",
        "gender_valid": "gender IS NULL OR gender IN ('male', 'female')",
    },
    comment="Customers",
)
