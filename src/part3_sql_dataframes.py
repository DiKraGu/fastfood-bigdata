import time
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
from spark_utils import get_spark_session

spark = get_spark_session("FastFood-Part3-SQL-DataFrames")

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("restaurant_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("city", StringType(), False),
    StructField("status", StringType(), False),
    StructField("amount_mad", DoubleType(), False),
    StructField("items_count", IntegerType(), False),
    StructField("distance_km", DoubleType(), False),
    StructField("courier_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
])

restaurants_schema = StructType([
    StructField("restaurant_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("category", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
])

t0 = time.time()
orders_df = spark.read.schema(orders_schema).json("s3a://lake/bronze/history/orders_history.jsonl")
restaurants_df = spark.read.option("multiline", "true").schema(restaurants_schema).json("s3a://lake/bronze/restaurants.json")

delivered_df = orders_df.filter(F.col("status") == "DELIVERED")
ca_df = (
    delivered_df
    .groupBy("city")
    .agg(
        F.count("*").alias("orders_count"),
        F.round(F.sum("amount_mad"), 2).alias("total_ca_mad"),
        F.round(F.avg("amount_mad"), 2).alias("avg_basket_mad")
    )
)
ca_df.show()
print(f"Temps DataFrame CA par ville : {time.time() - t0:.2f} s")

delivered_df.createOrReplaceTempView("orders_delivered")
spark.sql("""
    SELECT city,
           COUNT(*) AS orders_count,
           ROUND(SUM(amount_mad), 2) AS total_ca_mad,
           ROUND(AVG(amount_mad), 2) AS avg_basket_mad
    FROM orders_delivered
    GROUP BY city
    ORDER BY city
""").show()

rest_clean = restaurants_df.drop("city").withColumnRenamed("name", "restaurant_name")
enriched_df = delivered_df.join(rest_clean, on="restaurant_id", how="inner")

print("=== CA par Ville et Catégorie ===")
enriched_df.groupBy("city", "category").agg(
    F.count("*").alias("nb_orders"),
    F.round(F.sum("amount_mad"), 2).alias("ca_mad")
).orderBy("city", F.desc("ca_mad")).show(25)

@F.udf(returnType=StringType())
def distance_band_udf(dist):
    if dist is None:
        return "inconnu"
    if dist < 2.0:
        return "court"
    elif dist <= 5.0:
        return "moyen"
    return "long"

t_udf = time.time()
df_with_udf = enriched_df.withColumn("distance_band", distance_band_udf(F.col("distance_km")))
df_with_udf.groupBy("distance_band").count().collect()
print(f"Temps avec UDF Python : {time.time() - t_udf:.3f} s")

t_native = time.time()
df_native = enriched_df.withColumn(
    "distance_band",
    F.when(F.col("distance_km") < 2.0, "court")
     .when(F.col("distance_km") <= 5.0, "moyen")
     .otherwise("long")
)
df_native.groupBy("distance_band").count().collect()
print(f"Temps avec expressions natives when/otherwise : {time.time() - t_native:.3f} s")

(
    df_native
    .write
    .mode("overwrite")
    .partitionBy("city")
    .parquet("s3a://lake/silver/orders_history/")
)
print("Écriture Parquet Silver terminée.")

print("=== EXPLAIN (TRUE) SUR LA JOINTURE ===")
enriched_df.explain(True)

time.sleep(20)
spark.stop()