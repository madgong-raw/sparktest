#Inner Join - Gets all records from both the DataFrames where the join condition is met - Matching records from both the DataFrames
#Outer Join - Gets all records from both the DataFrames and fills in nulls for missing matches on either side - Not matching records from both the DataFrames
#left Join - Gets all records from the left DataFrame and fills in nulls for missing matches on the right DataFrame
#right Join - Gets all records from the right DataFrame and fills in nulls for missing matches on the left DataFrame
#left_outer Join - Gets all records from the left DataFrame and fills in nulls for missing matches on the right DataFrame
#right_outer Join - Gets all records from the right DataFrame and fills in nulls for missing matches on the left DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("MyTestApp").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
policies = spark.read.csv("Demo/policies.csv", header=True, inferSchema=True)
claims = spark.read.csv("Demo/claims.csv", header=True, inferSchema=True)
# policies.printSchema()
# claims.printSchema()
#inner join
# inner_join_df = policies.join(claims, on="policy_id", how="inner")
# inner_join_df.select("policy_id", "vehicle_id", "claim_id", "claim_type", "claim_amount").show()
#left outer join
left_outer_join_df = policies.join(claims, on="policy_id", how="left_outer")
# left_outer_join_df.select("policy_id", "vehicle_id", "claim_id", "claim_type", "claim_amount").show()
left_outer_join_df.where("Claim_id is Null").select("policy_id", "vehicle_id", "claim_id", "claim_type", "claim_amount").show()
#anti join - returns all records from the left DataFrame where there are no matching records in the right DataFrame
anti_join_df = policies.join(claims, on="policy_id", how="left_anti")
anti_join_df.select("policy_id", "vehicle_id").show()