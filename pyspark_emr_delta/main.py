import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    DoubleType,
)
from pyspark.sql import functions as f, Window

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from pyspark_emr_delta.utils import mul_double_udf
from pyspark_emr_delta.config import config


builder = (
    SparkSession.builder.appName(config.APP_NAME)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)

spark = None

if config.ENVIRONMENT == "DEVELOPMENT":
    spark = (
        configure_spark_with_delta_pip(
            builder,
            extra_packages=[
                "org.apache.hadoop:hadoop-aws:3.3.1",
            ],
        )
        .master("local[*]")
        .getOrCreate()
    )
else:
    spark = builder.getOrCreate()

sc = spark.sparkContext


df = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .load(str(config.DATA_PATH))
)

df = df.toDF(*list(map(lambda x: x.strip().lower().replace(" ", "_"), df.columns)))

"""
    In production use-cases:
    Write to s3 -> staging_<dt>_<table> overwrite on write
    Then next step -> from the staging_<dt>_<table> upsert into respective tables. On failure, retry upserts
"""

# total by city
total_by_city = (
    df.groupBy(f.col("city"))
    .agg(
        f.round(f.sum(f.col("total")), 2).alias("total_sales_amount"),
        f.count_distinct(f.col("product_line")).alias("num_unique_products"),
    )
    .orderBy(f.col("total_sales_amount").desc())
)

# total by branch
total_by_branch = (
    df.groupBy(
        f.concat(f.col("city"), f.lit("-"), f.col("branch")).alias("city_branch")
    )
    .agg(
        f.round(f.sum(f.col("total")), 2).alias("total_sales_amount"),
        f.count_distinct(f.col("product_line")).alias("num_unique_products"),
    )
    .orderBy(f.col("total_sales_amount").desc())
)


# top product lines by total products sold by city and branch
top_products = (
    df.groupBy(
        f.concat(f.col("city"), f.lit("-"), f.col("branch")).alias("city_branch"),
        f.col("product_line"),
    )
    .agg(f.count("*").alias("total_products"))
    .orderBy(f.col("total_products").desc())
)

# Top 2 products only
top_2_products = (
    df.groupBy(
        f.concat(f.col("city"), f.lit("-"), f.col("branch")).alias("city_branch"),
        f.col("product_line"),
    )
    .agg(f.count("*").alias("total_products"))
    .select(
        f.col("city_branch"),
        f.col("product_line"),
        f.col("total_products"),
        f.dense_rank()
        .over(
            Window.partitionBy(f.col("city_branch")).orderBy(
                f.col("total_products").desc()
            )
        )
        .alias("rnk"),
    )
    .where(f.col("rnk") <= 2)
    .orderBy(f.col("total_products").desc())
)


df.printSchema()
df.show(10)

total_by_city.show(20)
total_by_branch.show(20)
top_products.show(20)
top_2_products.show(20)

# save them as delta
# not partitioned, ideally partitioned by date
total_by_city.write.format("delta").mode("overwrite").save(
    str(config.OUT_PATH("total_city"))
)

# Read table and print to stdout
df_total_city = DeltaTable.forPath(spark, str(config.OUT_PATH("total_city")))
df_total_city.toDF().show()

# merges
# We'll upsert data for this row:
# {"city":"Naypyitaw","total_sales_amount":110568.71,"num_unique_products":6}
new_df = spark.createDataFrame(
    [("Naypyitaw", 120000.00, 10)],
    StructType(
        [
            StructField("city", StringType(), True),
            StructField("total_sales_amount", DoubleType(), True),
            StructField("num_unique_products", IntegerType(), True),
        ]
    ),
)

# apply UDF from package
new_df = new_df.select(
    f.col("city"),
    f.col("total_sales_amount"),
    mul_double_udf(f.col("num_unique_products")).alias("num_unique_products"),
)

(
    df_total_city.alias("old_data")
    .merge(source=new_df.alias("new_data"), condition="old_data.city = new_data.city")
    .whenMatchedUpdate(
        condition="new_data.total_sales_amount > 0",
        set={
            "total_sales_amount": f.col("new_data.total_sales_amount"),
            "num_unique_products": f.col("new_data.num_unique_products"),
        },
    )
    .whenNotMatchedInsert(
        values={
            "city": f.col("new_data.city"),
            "total_sales_amount": f.col("new_data.total_sales_amount"),
            "num_unique_products": f.col("new_data.num_unique_products"),
        }
    )
    .execute()  # writes back to the delta table as well
)


df_total_city.toDF().show(20)
