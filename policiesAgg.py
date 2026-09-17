from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("MyTestApp").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
policies = spark.read.csv("Demo/policies.csv", header=True, inferSchema=True)
# policies_total_premium_df = policies.select(F.sum("premium_amount")).alias("total_premium")
# policies_average_premium_df = policies.select(F.avg("premium_amount")).alias("average_premium")
# policies_minimum_premium_df = policies.select(F.min("premium_amount")).alias("minimum_premium")
# policies_maximum_premium_df = policies.select(F.max("premium_amount")).alias("maximum_premium")
# policies_total_policies_df = policies.select(F.count("policy_id")).alias("Total_Policies")
# policies_aggregates_df = policies.agg(
#     F.sum("premium_amount").alias("total_premium"),
#     F.avg("premium_amount").alias("average_premium"),
#     F.min("premium_amount").alias("minimum_premium"),
#     F.max("premium_amount").alias("maximum_premium"),
#     F.count("policy_id").alias("total_policies")
# )

# policies_aggregates_df.show(truncate=False)

high_premium_active_policies_df = policies.filter((F.col("premium_amount") > 1000) & (F.col("policy_status") == "Active"))
high_premium_active_policies_df.show(truncate=False)