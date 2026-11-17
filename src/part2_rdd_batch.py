import json
import time
from spark_utils import get_spark_session

spark = get_spark_session("FastFood-Part2-RDD")
sc = spark.sparkContext

t0 = time.time()

raw_rdd = sc.textFile("s3a://lake/bronze/history/orders_history.jsonl")

delivered_rdd = (
    raw_rdd
    .map(lambda line: json.loads(line))
    .filter(lambda order: order.get("status") == "DELIVERED")
)

delivered_rdd.cache()

avg_basket_rdd = (
    delivered_rdd
    .map(lambda o: (o["city"], (float(o["amount_mad"]), 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda acc: (round(acc[0], 2), acc[1], round(acc[0] / acc[1], 2)))
)

top5_restaurants = (
    delivered_rdd
    .map(lambda o: (o["restaurant_id"], 1))
    .reduceByKey(lambda a, b: a + b)
    .takeOrdered(5, key=lambda x: -x[1])
)

stats_city = avg_basket_rdd.collect()
elapsed = time.time() - t0

print("=== CA Total, Volume et Panier Moyen par Ville (RDD) ===")
for city, (total_ca, count, avg_basket) in sorted(stats_city):
    print(f"{city:<12} | Commandes: {count:<6} | CA: {total_ca:>10.2f} MAD | Panier moyen: {avg_basket:>6.2f} MAD")

print("\n=== Top 5 Restaurants (Commandes Livrées) ===")
for rest_id, cnt in top5_restaurants:
    print(f"{rest_id} : {cnt} commandes")

print("\n=== Lineage (toDebugString) ===")
print(avg_basket_rdd.toDebugString().decode("utf-8"))
print(f"\nTemps d'exécution Partie 2 (RDD) : {elapsed:.2f} s")

time.sleep(20)
spark.stop()