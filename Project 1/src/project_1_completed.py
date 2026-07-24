"""
Project 1 - Retail PySpark Pipeline

This script:
1. Reads the customers, products, and orders CSV files from Amazon S3.
2. Cleans and standardizes each dataset with PySpark.
3. Removes duplicate and invalid records.
4. Validates customer_id and product_id foreign keys.
5. Joins the three datasets and creates calculated sales fields.
6. Compares source and target record counts.
7. Writes the curated DataFrames to Apache Iceberg tables.
8. Exports Snowflake-ready CSV files back to Amazon S3.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# One base bucket keeps the raw input, Iceberg warehouse, and Snowflake exports
# together while separating them into different folders.
# for security I removed the link for my s3 bucket 
S3_BUCKET = "s3://######################-us-east-2-an"
RAW_DATA_PATH = S3_BUCKET
ICEBERG_WAREHOUSE_PATH = f"{S3_BUCKET}/iceberg/"
SNOWFLAKE_STAGE_PATH = f"{S3_BUCKET}/snowflake_stage/"


# =============================================================================
# 1. CREATE THE SPARK SESSION
# =============================================================================

spark = (
    SparkSession.builder
    .appName("RetailDataPipeline")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        ICEBERG_WAREHOUSE_PATH,
    )
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    # Invalid casts and impossible dates should become null so they can be cleaned.
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)


# =============================================================================
# 2. DEFINE THE RAW CSV SCHEMAS
# =============================================================================
#
# The column names and order come from the trainer's starter file.
#
# The raw columns are intentionally read as strings because the files contain
# dirty values such as:
#   - mixed dates: 2023-01-15, 2023/02/20, and 15-03-2023
#   - mixed booleans: TRUE, Yes, 1, Y, and No
#   - currency values: $14.99
#   - decimal commas: 399,99
#   - invalid numeric IDs: XYZ
#
# After the values are cleaned, the script casts them to the correct data types.

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("order_date", StringType()),
    StructField("ship_date", StringType()),
    StructField("quantity", StringType()),
    StructField("unit_price", StringType()),
    StructField("discount_pct", StringType()),
    StructField("total_amount", StringType()),
    StructField("payment_method", StringType()),
    StructField("order_status", StringType()),
])

products_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("price", StringType()),
    StructField("cost", StringType()),
    StructField("stock_quantity", StringType()),
    StructField("weight_kg", StringType()),
    StructField("created_date", StringType()),
    StructField("is_active", StringType()),
])

customers_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("signup_date", StringType()),
    StructField("country", StringType()),
    StructField("state", StringType()),
    StructField("postal_code", StringType()),
    StructField("is_active", StringType()),
    StructField("loyalty_points", StringType()),
])


# =============================================================================
# 3. READ THE RAW CSV FILES FROM S3
# =============================================================================

orders_df = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(orders_schema)
    .csv(f"{RAW_DATA_PATH}/orders.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(products_schema)
    .csv(f"{RAW_DATA_PATH}/products.csv")
)

customers_df = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(customers_schema)
    .csv(f"{RAW_DATA_PATH}/customers.csv")
)

print("RAW DATA SCHEMAS")
customers_df.printSchema()
products_df.printSchema()
orders_df.printSchema()


# =============================================================================
# 4. SMALL CLEANING HELPERS
# =============================================================================

def clean_text(column_name):
    """Trim a string and convert common null-like strings to real null values."""
    trimmed_value = F.trim(F.col(column_name))

    return (
        F.when(
            F.upper(trimmed_value).isin("", "NULL", "N/A", "NA", "NONE", "NAN"),
            F.lit(None),
        )
        .otherwise(trimmed_value)
    )


def parse_decimal(column_name, precision=12, scale=2):
    """Remove currency symbols and support both decimal points and decimal commas."""
    value_without_currency = F.regexp_replace(
        clean_text(column_name),
        r"[\$€£\s]",
        "",
    )

    standardized_decimal = F.when(
        (F.instr(value_without_currency, ",") > 0)
        & (F.instr(value_without_currency, ".") == 0),
        F.regexp_replace(value_without_currency, ",", "."),
    ).otherwise(
        F.regexp_replace(value_without_currency, ",", "")
    )

    return standardized_decimal.cast(DecimalType(precision, scale))


def parse_date(column_name, *date_formats):
    """Try each supplied date format and return the first valid date."""
    return F.coalesce(
        *[
            F.to_date(clean_text(column_name), date_format)
            for date_format in date_formats
        ]
    )


def parse_boolean(column_name):
    """Convert the boolean representations used in the CSV files."""
    boolean_text = F.lower(clean_text(column_name))

    return (
        F.when(boolean_text.isin("true", "yes", "y", "1"), F.lit(True))
        .when(boolean_text.isin("false", "no", "n", "0"), F.lit(False))
        .otherwise(F.lit(None).cast(BooleanType()))
    )


# =============================================================================
# 5. CLEAN THE CUSTOMERS DATA
# =============================================================================

customer_id_value = clean_text("customer_id").cast(IntegerType())
loyalty_points_value = clean_text("loyalty_points").cast(IntegerType())

email_value = F.lower(clean_text("email"))
email_pattern = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"

phone_digits = F.regexp_replace(clean_text("phone"), r"\D", "")
phone_digits = F.when(
    (F.length(phone_digits) == 11) & phone_digits.startswith("1"),
    F.substring(phone_digits, 2, 10),
).otherwise(phone_digits)

standard_phone = F.when(
    F.length(phone_digits) == 10,
    F.concat(
        F.substring(phone_digits, 1, 3),
        F.lit("-"),
        F.substring(phone_digits, 4, 3),
        F.lit("-"),
        F.substring(phone_digits, 7, 4),
    ),
).otherwise(F.lit(None).cast(StringType()))

country_key = F.upper(
    F.regexp_replace(clean_text("country"), r"[^A-Za-z]", "")
)

standard_country = (
    F.when(
        country_key.isin(
            "US",
            "USA",
            "UNITEDSTATES",
            "UNITEDSTATESOFAMERICA",
        ),
        F.lit("USA"),
    )
    .when(country_key == "CANADA", F.lit("CANADA"))
    .when(country_key == "MEXICO", F.lit("MEXICO"))
    .otherwise(F.upper(clean_text("country")))
)

state_value = F.upper(clean_text("state"))
standard_state = (
    F.when(state_value.isin("MINNESOTA", "MN"), F.lit("MN"))
    .when(state_value.isin("WISCONSIN", "WI"), F.lit("WI"))
    .when(state_value.isin("ONTARIO", "ON"), F.lit("ON"))
    .otherwise(state_value)
)

customers_df_clean = (
    customers_df
    .withColumn("customer_id", customer_id_value)
    .withColumn("first_name", F.initcap(F.lower(clean_text("first_name"))))
    .withColumn("last_name", F.initcap(F.lower(clean_text("last_name"))))
    .withColumn(
        "email",
        F.when(email_value.rlike(email_pattern), email_value)
        .otherwise(F.lit(None).cast(StringType())),
    )
    .withColumn("phone", standard_phone)
    .withColumn(
        "signup_date",
        parse_date(
            "signup_date",
            "yyyy-MM-dd",
            "yyyy/MM/dd",
            "dd-MM-yyyy",
        ),
    )
    .withColumn("country", standard_country)
    .withColumn("state", standard_state)
    .withColumn("postal_code", clean_text("postal_code"))
    .withColumn("is_active", parse_boolean("is_active"))
    .withColumn(
        "loyalty_points",
        F.when(loyalty_points_value < 0, F.lit(0))
        # Project rule: values above 100,000 are treated as impossible.
        .when(
            loyalty_points_value > 100000,
            F.lit(None).cast(IntegerType()),
        )
        .otherwise(loyalty_points_value),
    )
    # A customer record must have a valid numeric customer ID.
    .filter(F.col("customer_id").isNotNull())
    # One final customer row per customer ID.
    .dropDuplicates(["customer_id"])
)


# =============================================================================
# 6. CLEAN THE PRODUCTS DATA
# =============================================================================

price_value = parse_decimal("price")
cost_value = parse_decimal("cost")
stock_quantity_value = clean_text("stock_quantity").cast(IntegerType())
weight_value = parse_decimal("weight_kg", precision=8, scale=3)
brand_value = clean_text("brand")

products_df_clean = (
    products_df
    .withColumn("product_id", F.upper(clean_text("product_id")))
    # Keep intentional acronyms such as USB, LED, TV, and SSD unchanged.
    .withColumn("product_name", clean_text("product_name"))
    .withColumn("category", F.initcap(F.lower(clean_text("category"))))
    .withColumn(
        "brand",
        F.when(F.upper(brand_value) == "IKEA", F.lit("IKEA"))
        .otherwise(brand_value),
    )
    .withColumn(
        "price",
        F.when(price_value > 0, price_value)
        .otherwise(F.lit(None).cast(DecimalType(12, 2))),
    )
    .withColumn(
        "cost",
        F.when(cost_value >= 0, cost_value)
        .otherwise(F.lit(None).cast(DecimalType(12, 2))),
    )
    # Inventory cannot be negative, so a negative value becomes zero.
    .withColumn(
        "stock_quantity",
        F.when(stock_quantity_value < 0, F.lit(0))
        .otherwise(stock_quantity_value),
    )
    .withColumn(
        "weight_kg",
        F.when(weight_value > 0, weight_value)
        .otherwise(F.lit(None).cast(DecimalType(8, 3))),
    )
    .withColumn(
        "created_date",
        parse_date(
            "created_date",
            "yyyy-MM-dd",
            "yyyy/MM/dd",
        ),
    )
    .withColumn("is_active", parse_boolean("is_active"))
    # A valid product ID starts with P and contains four digits.
    .filter(F.col("product_id").rlike(r"^P\d{4}$"))
    # One final product row per product ID.
    .dropDuplicates(["product_id"])
)


# =============================================================================
# 7. CLEAN THE ORDERS DATA
# =============================================================================

order_id_value = clean_text("order_id").cast(IntegerType())
order_customer_id_value = clean_text("customer_id").cast(IntegerType())
quantity_value = clean_text("quantity").cast(IntegerType())
unit_price_value = parse_decimal("unit_price")
discount_value = parse_decimal("discount_pct", precision=5, scale=2)
source_total_value = parse_decimal("total_amount")

payment_key = F.lower(
    F.regexp_replace(clean_text("payment_method"), r"\s+", "")
)

standard_payment_method = (
    F.when(payment_key.isNull(), F.lit("Unknown"))
    .when(
        payment_key.isin("creditcard", "visa", "mastercard"),
        F.lit("Credit Card"),
    )
    .when(payment_key.isin("debit", "debitcard"), F.lit("Debit Card"))
    .when(payment_key == "paypal", F.lit("PayPal"))
    .when(payment_key == "applepay", F.lit("Apple Pay"))
    .when(payment_key == "googlepay", F.lit("Google Pay"))
    .when(payment_key == "cash", F.lit("Cash"))
    .otherwise(F.initcap(clean_text("payment_method")))
)

orders_df_values_clean = (
    orders_df
    .withColumn("order_id", order_id_value)
    .withColumn("customer_id", order_customer_id_value)
    .withColumn("product_id", F.upper(clean_text("product_id")))
    .withColumn(
        "order_date",
        parse_date(
            "order_date",
            "yyyy-MM-dd",
            "yyyy/MM/dd",
            "MM-dd-yyyy",
        ),
    )
    .withColumn(
        "ship_date",
        parse_date(
            "ship_date",
            "yyyy-MM-dd",
            "yyyy/MM/dd",
            "MM-dd-yyyy",
        ),
    )
    .withColumn("quantity", quantity_value)
    .withColumn("unit_price", unit_price_value)
    # A missing or nonnumeric discount means that no discount was applied.
    .withColumn(
        "discount_pct",
        F.when(discount_value.isNull(), F.lit(0).cast(DecimalType(5, 2)))
        .otherwise(discount_value),
    )
    .withColumn("source_total_amount", source_total_value)
    .withColumn("payment_method", standard_payment_method)
    .withColumn(
        "order_status",
        F.coalesce(
            F.initcap(F.lower(clean_text("order_status"))),
            F.lit("Unknown"),
        ),
    )
    # One final order row per order ID.
    .dropDuplicates(["order_id"])
)


# Keep only orders with valid required values.
#
# Project rule: a single retail order line may contain between 1 and 100 units.
# This identifies the quantity of 1,000 in the raw file as an impossible value.
orders_df_valid = orders_df_values_clean.filter(
    F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("product_id").rlike(r"^P\d{4}$")
    & F.col("order_date").isNotNull()
    & (
        F.col("ship_date").isNull()
        | (F.col("ship_date") >= F.col("order_date"))
    )
    & F.col("quantity").between(1, 100)
    & (F.col("unit_price") > 0)
    & F.col("discount_pct").between(0, 100)
)


# Validate the foreign keys.
#
# An order is kept only if its customer_id exists in the cleaned customers
# dataset and its product_id exists in the cleaned products dataset.
valid_customer_ids = customers_df_clean.select("customer_id")
valid_product_ids = products_df_clean.select("product_id")

orders_df_clean = (
    orders_df_valid
    .join(valid_customer_ids, "customer_id", "inner")
    .join(valid_product_ids, "product_id", "inner")
    .withColumn(
        "calculated_total_amount",
        F.round(
            F.col("quantity")
            * F.col("unit_price")
            * (F.lit(1) - (F.col("discount_pct") / F.lit(100))),
            2,
        ).cast(DecimalType(14, 2)),
    )
    .withColumn(
        "amount_difference",
        F.when(
            F.col("source_total_amount").isNotNull(),
            F.round(
                F.abs(
                    F.col("source_total_amount")
                    - F.col("calculated_total_amount")
                ),
                2,
            ),
        ).otherwise(F.lit(None).cast(DecimalType(14, 2))),
    )
    .select(
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "ship_date",
        "quantity",
        "unit_price",
        "discount_pct",
        "source_total_amount",
        "calculated_total_amount",
        "amount_difference",
        "payment_method",
        "order_status",
    )
)


# =============================================================================
# 8. JOIN THE DATASETS AND CREATE CALCULATED FIELDS
# =============================================================================

customer_lookup = customers_df_clean.select(
    "customer_id",
    F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
    "email",
    "country",
    "state",
)

product_lookup = products_df_clean.select(
    "product_id",
    "product_name",
    "category",
    "brand",
    F.col("price").alias("list_price"),
    "cost",
)

sales_enriched_df = (
    orders_df_clean
    .join(customer_lookup, "customer_id", "inner")
    .join(product_lookup, "product_id", "inner")
    .withColumn(
        "gross_sales",
        F.round(
            F.col("quantity") * F.col("unit_price"),
            2,
        ).cast(DecimalType(14, 2)),
    )
    .withColumn(
        "discount_amount",
        F.round(
            F.col("gross_sales") * F.col("discount_pct") / F.lit(100),
            2,
        ).cast(DecimalType(14, 2)),
    )
    .withColumn(
        "net_sales",
        F.col("calculated_total_amount"),
    )
    .withColumn(
        "total_cost",
        F.round(
            F.col("quantity") * F.col("cost"),
            2,
        ).cast(DecimalType(14, 2)),
    )
    .withColumn(
        "profit",
        F.round(
            F.col("net_sales") - F.col("total_cost"),
            2,
        ).cast(DecimalType(14, 2)),
    )
    .withColumn(
        "shipping_days",
        F.when(
            F.col("ship_date").isNotNull(),
            F.datediff(F.col("ship_date"), F.col("order_date")),
        ),
    )
    .withColumn(
        "order_month",
        F.date_format(F.col("order_date"), "yyyy-MM"),
    )
    .select(
        "order_id",
        "order_date",
        "order_month",
        "ship_date",
        "shipping_days",
        "order_status",
        "payment_method",
        "customer_id",
        "customer_name",
        "email",
        "country",
        "state",
        "product_id",
        "product_name",
        "category",
        "brand",
        "quantity",
        "list_price",
        "unit_price",
        "cost",
        "discount_pct",
        "gross_sales",
        "discount_amount",
        "net_sales",
        "total_cost",
        "profit",
    )
)


# =============================================================================
# 9. VALIDATE SOURCE AND TARGET RECORD COUNTS
# =============================================================================

customer_source_count = customers_df.count()
customer_target_count = customers_df_clean.count()

product_source_count = products_df.count()
product_target_count = products_df_clean.count()

order_source_count = orders_df.count()
order_target_count = orders_df_clean.count()

data_quality_summary_df = (
    spark.createDataFrame(
        [
            (
                "customers",
                customer_source_count,
                customer_target_count,
            ),
            (
                "products",
                product_source_count,
                product_target_count,
            ),
            (
                "orders",
                order_source_count,
                order_target_count,
            ),
        ],
        [
            "dataset",
            "source_record_count",
            "target_record_count",
        ],
    )
    .withColumn(
        "removed_record_count",
        F.col("source_record_count") - F.col("target_record_count"),
    )
)


# =============================================================================
# 10. CREATE THE ICEBERG DATABASE AND WRITE THE CURATED TABLES
# =============================================================================

spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db
""")

iceberg_tables = {
    "customers": customers_df_clean,
    "products": products_df_clean,
    "orders": orders_df_clean,
    "sales_enriched": sales_enriched_df,
    "data_quality_summary": data_quality_summary_df,
}

for table_name, dataframe in iceberg_tables.items():
    (
        dataframe.writeTo(
            f"glue_catalog.iceberg_catalog_db.{table_name}"
        )
        .using("iceberg")
        .createOrReplace()
    )

    print(f"Wrote Iceberg table: {table_name}")


# =============================================================================
# 11. EXPORT CURATED CSV FILES FOR SNOWFLAKE
# =============================================================================

# Snowflake will use the external stage created in 01_snowflake_ddl.sql to read
# these four folders. coalesce(1) is acceptable here because this course dataset
# is small and it produces one CSV data file per output folder.
snowflake_exports = {
    "dim_customers": customers_df_clean,
    "dim_products": products_df_clean,
    "fact_orders": orders_df_clean,
    "dq_summary": data_quality_summary_df,
}

for folder_name, dataframe in snowflake_exports.items():
    output_path = f"{SNOWFLAKE_STAGE_PATH}{folder_name}/"

    (
        dataframe.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("nullValue", "")
        .option("dateFormat", "yyyy-MM-dd")
        .csv(output_path)
    )

    print(f"Wrote Snowflake CSV export: {output_path}")


# =============================================================================
# 12. DISPLAY PIPELINE RESULTS
# =============================================================================

print("SOURCE-TO-TARGET RECORD COUNT VALIDATION")
data_quality_summary_df.show(truncate=False)

print("ENRICHED SALES DATA")
sales_enriched_df.orderBy("order_id").show(truncate=False)

print("PySpark processing is complete.")
print("Run 01_snowflake_ddl.sql through 04_analytics_queries.sql in order.")


# Stop the Spark session and release the cluster resources.
spark.stop()
