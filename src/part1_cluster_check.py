from spark_utils import get_spark_session

spark = get_spark_session("FastFood-Part1-ClusterCheck")
sc = spark.sparkContext

print(f"Spark Version      : {spark.version}")
print(f"Master URL         : {sc.master}")
print(f"App Name           : {sc.appName}")
print(f"Default Parallelism: {sc.defaultParallelism}")

spark.stop()