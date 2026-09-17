from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("MyTestApp").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
policies = spark.read.csv("../Demo/policies.csv", header=True, inferSchema=True)
claims = spark.read.csv("../Demo/claims.csv", header=True, inferSchema=True)
vehicles = spark.read.csv("../Demo/vehicles.csv", header=True, inferSchema=True)
customers = spark.read.csv("../Demo/customers.csv", header=True, inferSchema=True)
# policies.printSchema()
# claims.printSchema()
#JOINS: Join claims with policies using policy_id & display given columns.
policies_claims_df = policies.join(claims, on="policy_id", how="inner")
policies_claims_df.select("claim_id", "policy_id", "claim_type", "claim_amount", "policy_type", "premium_amount", "sum_insured", "policy_status").show(10)

#Perform a left join between policies and claims and identify the policies that do not have claims.
left_join_df = policies.join(claims, on="policy_id", how="left")
left_join_df.where("claim_id is Null").select("policy_id", "vehicle_id", "policy_type", "premium_amount", "sum_insured", "policy_status").show(10)

#Join policies with vehicles using vehicle_id. 
policies_vehicles_df = policies.join(vehicles, on="vehicle_id", how="inner")
policies_vehicles_df.select("policy_id", "vehicle_id", "policy_type", "premium_amount", "make", "model", "vehicle_value").show(10)

# Analyze Claims by Customer Segment: Using the multi-table joined DataFrame, group by customer_segment and calculate:
# 	Number of claims
# 	Total claim amount
# 	Total approved amount
# 	Average claim amount
# here we have to use policies_claims_df and join it with customers to get vehicle_id and customer_id and then join with Customers to get customer_segment
claims_customer_segment_df = policies_claims_df.join(vehicles, on="vehicle_id", how="inner").join(customers, on="customer_id", how="inner")
claims_customer_segment_agg_df = claims_customer_segment_df.groupBy("customer_segment").agg(F.count("claim_id").alias("num_claims"), F.sum("claim_amount").alias("total_claim_amount"), F.sum("approved_amount").alias("total_approved_amount"), F.avg("claim_amount").alias("avg_claim_amount"))
claims_customer_segment_agg_df.show(5)


#Identify High-Risk Claims: Using the multi-table joined DataFrame, display claims where:
	# Claim amount is greater than ₹200,000, and
	# Fraud flag is 1 or claim status is "Under Review".
	# Display customer and vehicle details along with the claim information.
high_risk_claims_df = policies_claims_df.join(vehicles, on="vehicle_id", how="inner").join(customers, on="customer_id", how="inner").filter((F.col("claim_amount") > 200000) & ((F.col("fraud_flag") == 1) | (F.col("claim_status") == "Under Review")))
high_risk_claims_df.select("claim_id", "policy_id", "customer_name", "city", "make", "model", "claim_amount", "approved_amount", "claim_status", "fraud_flag").show(10)

#Fraud Investigation Report: Create a report containing fraud-flagged claims with:
	# Customer name
	# City
	# Vehicle make and model
	# Claim amount
	# Approved amount
	# Claim status
	# Policy type

fraud_investigation_report_df = policies_claims_df.join(vehicles, on="vehicle_id", how="inner").join(customers, on="customer_id", how="inner").filter(F.col("fraud_flag") == 1)
fraud_investigation_report_df.select("claim_id", "policy_id", "customer_name", "city", "make", "model", "claim_amount", "approved_amount", "claim_status", "policy_type").show(10)

#Using the final joined DataFrame, answer:
	# Which customer segment has the highest total claim amount?
	# Which city has the highest total claim amount?
	# Which vehicle model has the highest total claim amount?
	# Which policies have multiple claims?
	# Which policies have no claims?
	# Which policies were not renewed?
	# Which policies have pending payments?
	# Which claims are fraud-flagged?
	# Which claims have the highest outstanding amount?
	# Which policy has the highest claim amount compared with its sum insured?
joined_df = policies_claims_df.join(vehicles, on="vehicle_id", how="inner").join(customers, on="customer_id", how="inner")
# # Which customer segment has the highest total claim amount?
customer_segment_highest_claim_df = joined_df.groupBy("customer_segment").agg(F.sum("claim_amount").alias("total_claim_amount")).orderBy(F.col("total_claim_amount").desc())
customer_segment_highest_claim_df.show(1)  
city_highest_claim_df = joined_df.groupBy("city").agg(F.sum("claim_amount").alias("total_claim_amount")).orderBy(F.col("total_claim_amount").desc())
city_highest_claim_df.show(1)
vehicle_model_highest_claim_df = joined_df.groupBy("model").agg(F.sum("claim_amount").alias("total_claim_amount")).orderBy(F.col("total_claim_amount").desc())
vehicle_model_highest_claim_df.show(1)
policies_multiple_claims_df = joined_df.groupBy("policy_id").agg(F.count("claim_id").alias("num_claims")).filter(F.col("num_claims") > 1)
policies_multiple_claims_df.show(10)
policies_no_claims_df = policies.join(claims, on="policy_id", how="left").filter(F.col("claim_id").isNull())
policies_no_claims_df.select("policy_id", "vehicle_id", "policy_type", "premium_amount", "sum_insured", "policy_status").show(10)   
policies_not_renewed_df = policies.filter(F.col("policy_status") == "Not Renewed")
policies_not_renewed_df.show(10)
policies_pending_payments_df = policies.join(claims, on="policy_id", how="left").filter(F.col("claim_status") == "Pending")
policies_pending_payments_df.select("policy_id", "vehicle_id", "policy_type", "premium_amount", "sum_insured", "policy_status").show(10)
fraud_flagged_claims_df = joined_df.filter(F.col("fraud_flag") == 1)
fraud_flagged_claims_df.select("claim_id", "policy_id", "customer_segment", "claim_amount", "fraud_flag", "claim_status").show(10)
highest_outstanding_claims_df = joined_df.withColumn("outstanding_amount", F.col("claim_amount") - F.col("approved_amount")).orderBy(F.col("outstanding_amount").desc())
highest_outstanding_claims_df.select("claim_id", "policy_id", "customer_segment", "claim_amount", "approved_amount", "outstanding_amount").show(10)
highest_claim_vs_sum_insured_df = joined_df.withColumn("claim_to_sum_insured_ratio", F.col("claim_amount") / F.col("sum_insured")).orderBy(F.col("claim_to_sum_insured_ratio").desc())
highest_claim_vs_sum_insured_df.select("policy_id", "claim_amount", "sum_insured", "claim_to_sum_insured_ratio").show(10)
