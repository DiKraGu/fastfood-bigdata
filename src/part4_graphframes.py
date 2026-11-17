from pyspark.sql import functions as F
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from spark_utils import get_spark_session

spark = get_spark_session("FastFood-Part4-GraphFrames")
# Évite l'explosion exponentielle O(2^k) de explainString dans AdaptiveSparkPlanExec.onUpdatePlan
spark.conf.set("spark.sql.adaptive.enabled", "false")

sc = spark.sparkContext
sc.setCheckpointDir("s3a://lake/checkpoints/")

# 1. Lecture des données Silver (commandes DELIVERED uniquement)
silver_df = spark.read.parquet("s3a://lake/silver/orders_history/")
restaurants_df = spark.read.option("multiline", "true").json("s3a://lake/bronze/restaurants.json")

# Construction des sommets (vertices) : clients + restaurants
customers_v = (
    silver_df.select(F.col("customer_id").alias("id"))
    .distinct()
    .withColumn("type", F.lit("client"))
    .withColumn("city", F.lit(None).cast("string"))
    .withColumn("category", F.lit(None).cast("string"))
)

restaurants_v = (
    restaurants_df.select(
        F.col("restaurant_id").alias("id"),
        F.lit("restaurant").alias("type"),
        F.col("city"),
        F.col("category"),
    )
)

vertices = customers_v.unionByName(restaurants_v).cache()

# Construction des arêtes (edges) : client -> restaurant agrégées par couple
edges = (
    silver_df.groupBy(
        F.col("customer_id").alias("src"),
        F.col("restaurant_id").alias("dst"),
    )
    .agg(
        F.count("*").alias("orders_count"),
        F.round(F.sum("amount_mad"), 2).alias("total_amount"),
    )
    .cache()
)

g = GraphFrame(vertices, edges)
print("=== 1. Dimensions du Graphe ===")
print(f"Nombre de sommets (vertices) : {g.vertices.count()}")
print(f"Nombre d'arêtes (edges)      : {g.edges.count()}")

# 2. Degrés entrants et sortants
print("\n=== 2a. Top 10 Restaurants par Degré Entrant (clients distincts) ===")
(
    g.inDegrees
    .join(restaurants_v, on="id", how="inner")
    .select("id", "city", "category", F.col("inDegree").alias("distinct_customers"))
    .orderBy(F.desc("distinct_customers"), F.asc("id"))
    .show(10, truncate=False)
)

print("=== 2b. Top 10 Clients par Degré Sortant (restaurants distincts fréquentés) ===")
(
    g.outDegrees
    .orderBy(F.desc("outDegree"), F.asc("id"))
    .show(10, truncate=False)
)

# 3. PageRank (resetProbability=0.15, maxIter=10)
print("=== 3. Top 10 Restaurants les plus influents (PageRank) ===")
pr = g.pageRank(resetProbability=0.15, maxIter=10)
(
    pr.vertices
    .filter(F.col("type") == "restaurant")
    .select("id", "city", "category", F.round("pagerank", 4).alias("pagerank"))
    .orderBy(F.desc("pagerank"))
    .show(10, truncate=False)
)

# 4. Composantes connexes
print("=== 4. Composantes Connexes ===")
cc = g.connectedComponents(checkpointInterval=1)
cc_summary = (
    cc.groupBy("component")
    .agg(F.count("*").alias("component_size"))
    .orderBy(F.desc("component_size"))
)
print(f"Nombre total de composantes connexes : {cc_summary.count()}")
cc_summary.show(10, truncate=False)

# 5. Recherche de motifs : clients ayant commandé dans 2 restaurants distincts de même catégorie
print("=== 5a. Motifs : Clients fidèles à une catégorie sur 2 restaurants distincts ===")
motifs = g.find("(c)-[e1]->(r1); (c)-[e2]->(r2)").filter(
    (F.col("c.type") == "client")
    & (F.col("r1.type") == "restaurant")
    & (F.col("r2.type") == "restaurant")
    & (F.col("r1.id") != F.col("r2.id"))
    & (F.col("r1.category") == F.col("r2.category"))
)
motifs.select(
    F.col("c.id").alias("customer_id"),
    F.col("r1.id").alias("rest_1"),
    F.col("r2.id").alias("rest_2"),
    F.col("r1.category").alias("category"),
    F.col("r1.city").alias("city_r1"),
    F.col("r2.city").alias("city_r2"),
).show(10, truncate=False)

sample_row = motifs.select(
    F.col("c.id").alias("cid"),
    F.col("r1.category").alias("fav_cat"),
    F.col("r1.city").alias("fav_city"),
).first()

if sample_row:
    target_cid, fav_cat, fav_city = sample_row["cid"], sample_row["fav_cat"], sample_row["fav_city"]
    visited = edges.filter(F.col("src") == target_cid).select("dst")
    reco = (
        restaurants_v
        .filter((F.col("category") == fav_cat) & (F.col("city") == fav_city))
        .join(visited, restaurants_v.id == visited.dst, how="left_anti")
        .join(
            pr.vertices.select("id", F.round("pagerank", 4).alias("pagerank")),
            on="id",
            how="left",
        )
        .orderBy(F.desc("pagerank"))
    )
    print(f"=== 5b. Recommandation pour {target_cid} (catégorie préférée: {fav_cat}, ville: {fav_city}) ===")
    reco.show(5, truncate=False)

# 6. aggregateMessages : CA total reçu par chaque restaurant + vérification par groupBy
print("=== 6. CA Total par Restaurant : aggregateMessages vs groupBy ===")
agg_msg_df = (
    g.aggregateMessages(
        F.round(F.sum(AM.msg), 2).alias("ca_aggregate_messages"),
        sendToDst=AM.edge["total_amount"],
    )
    .join(restaurants_v.select("id", "city", "category"), on="id", how="inner")
)

groupby_df = (
    edges.groupBy(F.col("dst").alias("id"))
    .agg(F.round(F.sum("total_amount"), 2).alias("ca_groupby"))
)

comparison_df = (
    agg_msg_df
    .join(groupby_df, on="id", how="inner")
    .withColumn("diff", F.round(F.abs(F.col("ca_aggregate_messages") - F.col("ca_groupby")), 4))
    .orderBy(F.desc("ca_aggregate_messages"))
)
comparison_df.show(10, truncate=False)

max_diff = comparison_df.agg(F.max("diff")).collect()[0][0]
print(f"Écart maximal entre aggregateMessages et groupBy : {max_diff} MAD")

spark.stop()