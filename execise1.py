from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("MyTestApp").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
claims = spark.read.csv("Demo/claims.csv", header=True, inferSchema=True)
#Display only the following columns from the claims DataFrame:
claims_df = claims.select("claim_id","policy_id","claim_date","claim_type","claim_amount","approved_amount","claim_status")
print("Display only the following columns from the claims DataFrame:", claims_df.show())

#Filter High-Value Claims:Display claims where claim_amount is greater than ₹200,000.
high_value_claims = claims.filter(F.col("claim_amount") > 200000)
print("High-Value Claims:", high_value_claims.show())

#Filter Fraud-Flagged Claims: Display claims where fraud_flag is equal to 1.
fraud_flagged_claims = claims.filter(F.col("fraud_flag") == 1)
print("Fraud-Flagged Claims:", fraud_flagged_claims.show())

#Filter Claims Under Review: Display claims where claim_status is "Under Review".
under_review_claims = claims.filter(F.col("claim_status") == "Under Review")
print("Claims Under Review:", under_review_claims.show())

#Sort Claims: Sort claims by claim_amount in descending order.
sorted_claims = claims.orderBy(F.col("claim_amount").desc())
print("Sorted Claims by Claim Amount (Descending):", sorted_claims.show())

#Rename claim_amount to requested_amount and approved_amount to settled_amount.
claims_renamed = claims.withColumnRenamed("claim_amount", "requested_amount").withColumnRenamed("approved_amount", "settled_amount")
print("Claims with Renamed Columns:", claims_renamed.show())

#Remove Unnecessary Columns: Create a new DataFrame by removing claim_year and claim_month from the prepared claims DataFrame.
claims_filtered = claims.drop("claim_year", "claim_month")
print("Claims with Unnecessary Columns Removed:", claims_filtered.show())

#Overall Claim Metrics: Calculate:
# Total number of claims, Total claim amount, Total approved amount, Total outstanding amount, Average claim amount, Maximum claim amount, Minimum claim amount
claim_metrics = claims.agg(
    F.count("claim_id").alias("total_claims"),
    F.sum("claim_amount").alias("total_claim_amount"),
    F.sum("approved_amount").alias("total_approved_amount"),
    F.sum(F.col("claim_amount") - F.col("approved_amount")).alias("total_outstanding_amount"),
    F.avg("claim_amount").alias("average_claim_amount"),
    F.max("claim_amount").alias("maximum_claim_amount"),
    F.min("claim_amount").alias("minimum_claim_amount")
)
print("Overall Claim Metrics:", claim_metrics.show())