# import time
# from pyspark.sql import functions as F
# from pyspark.sql.types import (
#     StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
# )
# from spark_utils import get_spark_session

# spark = get_spark_session("FastFood-Part5-StructuredStreaming")
# spark.sparkContext.setLogLevel("WARN")

# orders_schema = StructType([
#     StructField("order_id", StringType(), False),
#     StructField("restaurant_id", StringType(), False),
#     StructField("customer_id", StringType(), False),
#     StructField("city", StringType(), False),
#     StructField("status", StringType(), False),
#     StructField("amount_mad", DoubleType(), False),
#     StructField("items_count", IntegerType(), False),
#     StructField("distance_km", DoubleType(), False),
#     StructField("courier_id", StringType(), False),
#     StructField("event_time", TimestampType(), False),
# ])

# # 1. Lecture du topic Kafka 'orders'
# raw_kafka_df = (
#     spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "kafka:9092")
#     .option("subscribe", "orders")
#     .option("startingOffsets", "latest")
#     .load()
# )

# # 2. Désérialisation JSON avec le schéma explicite de la Partie 3
# parsed_df = (
#     raw_kafka_df
#     .select(
#         F.col("key").cast("string").alias("kafka_key"),
#         F.col("partition"),
#         F.col("offset"),
#         F.col("timestamp").alias("kafka_ingest_time"),
#         F.from_json(F.col("value").cast("string"), orders_schema).alias("data")
#     )
#     .select("kafka_key", "partition", "offset", "kafka_ingest_time", "data.*")
# )

# # 3. Sink Console 1 : Flux brut en mode append (affiche aussi les événements en retard > 10 min)
# raw_with_delay_df = parsed_df.withColumn(
#     "delay_sec",
#     F.unix_timestamp("kafka_ingest_time") - F.unix_timestamp("event_time")
# )

# query_console_raw = (
#     raw_with_delay_df
#     .select("order_id", "city", "status", "amount_mad", "event_time", "kafka_ingest_time", "delay_sec")
#     .writeStream
#     .queryName("raw_orders_console")
#     .outputMode("append")
#     .format("console")
#     .option("truncate", "false")
#     .option("numRows", 15)
#     .trigger(processingTime="10 seconds")
#     .start()
# )

# # 4. Sink Bronze : Écriture du flux brut (JSON) dans MinIO avec checkpoint dédié
# query_bronze_json = (
#     raw_kafka_df
#     .select(
#         F.col("key").cast("string").alias("key"),
#         F.col("value").cast("string").alias("raw_json"),
#         F.col("partition"),
#         F.col("offset"),
#         F.col("timestamp").alias("kafka_timestamp")
#     )
#     .writeStream
#     .queryName("bronze_orders_json")
#     .outputMode("append")
#     .format("json")
#     .option("path", "s3a://lake/bronze/orders_raw/")
#     .option("checkpointLocation", "s3a://lake/checkpoints/part5_bronze_raw/")
#     .trigger(processingTime="10 seconds")
#     .start()
# )

# # 5 & 6. Agrégation avec Watermark de 10 minutes sur event_time (outputMode complete)
# # Note : avec watermark + fenêtre temporelle, Spark maintient l'état jusqu'à (max_event_time - 10 min).
# # Tout événement arrivant avec > 10 min de retard (ex: 15 min) est ignoré dans l'agrégation fenêtre.
# windowed_counts_df = (
#     parsed_df
#     .withWatermark("event_time", "10 minutes")
#     .groupBy(
#         F.window("event_time", "5 minutes"),
#         F.col("city"),
#         F.col("status")
#     )
#     .agg(
#         F.count("*").alias("orders_count"),
#         F.round(F.sum("amount_mad"), 2).alias("total_ca_mad")
#     )
#     .orderBy(F.desc("window"), F.asc("city"), F.asc("status"))
# )

# query_console_agg = (
#     windowed_counts_df
#     .writeStream
#     .queryName("agg_city_status_complete")
#     .outputMode("complete")
#     .format("console")
#     .option("truncate", "false")
#     .option("numRows", 25)
#     .trigger(processingTime="10 seconds")
#     .start()
# )

# print("Streaming Partie 5 démarré (3 requêtes actives). Laissez tourner ~45-60s puis Ctrl+C...")
# try:
#     spark.streams.awaitAnyTermination()
# except KeyboardInterrupt:
#     print("\nArrêt propre des streams Partie 5...")
#     for q in spark.streams.active:
#         q.stop()
#     spark.stop()

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
from spark_utils import get_spark_session

spark = get_spark_session("FastFood-Part5-StructuredStreaming")
spark.sparkContext.setLogLevel("WARN")

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

# 1. Lecture du flux Kafka
raw_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# 2. Parsing JSON + calcul du retard (kafka_ingest_time - event_time) + Watermark 10 min
events_with_watermark = (
    raw_kafka
    .select(
        F.from_json(F.col("value").cast("string"), orders_schema).alias("data"),
        F.col("timestamp").alias("kafka_ingest_time")
    )
    .select("data.*", "kafka_ingest_time")
    .filter(F.col("order_id").isNotNull() & F.col("event_time").isNotNull())
    .withColumn(
        "delay_sec",
        F.unix_timestamp("kafka_ingest_time") - F.unix_timestamp("event_time")
    )
    .withWatermark("event_time", "10 minutes")
)

# 3. Flux d'agrégation fenêtrée (5 minutes par ville et statut)
agg_df = (
    events_with_watermark
    .groupBy(
        F.window("event_time", "5 minutes"),
        F.col("city"),
        F.col("status")
    )
    .agg(
        F.count("order_id").alias("orders_count"),
        F.round(F.sum("amount_mad"), 2).alias("total_ca_mad")
    )
)

# 4. Flux de debug explicite : filtre sur les événements en retard (delay_sec > 0)
# pour garantir qu'on voit immédiatement les ACCEPTE (< 600s) et REJETE (> 600s)
debug_console_df = (
    events_with_watermark
    .filter(F.col("delay_sec") > 0)
    .withColumn(
        "watermark_status",
        F.when(F.col("delay_sec") > 600, "REJETE (Hors Watermark > 10m)")
         .otherwise("ACCEPTE (Dans Watermark <= 10m)")
    )
    .select(
        "order_id", "city", "status", "amount_mad",
        "event_time", "kafka_ingest_time", "delay_sec", "watermark_status"
    )
)

# 5. Lancement des deux requêtes console
query_debug = (
    debug_console_df.writeStream
    .queryName("debug_evenements_tardifs")
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 20)
    .trigger(processingTime="10 seconds")
    .start()
)

query_agg = (
    agg_df.writeStream
    .queryName("agregation_fenetres_5min")
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 25)
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming Partie 5 démarré. Surveillez la colonne 'watermark_status'...")
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nArrêt du streaming Partie 5...")
    for q in spark.streams.active:
        q.stop()
    spark.stop()