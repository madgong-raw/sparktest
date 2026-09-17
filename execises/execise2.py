from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("MyTestApp").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
claims = spark.read.csv("Demo/claims.csv", header=True, inferSchema=True)

#Overall Claims metrics
claim_metrics = claims.agg(
    F.count("claim_id").alias("total_claims"),
    F.sum("claim_amount").alias("total_claim_amount"),
    F.sum("approved_amount").alias("total_approved_amount"),
    F.sum(F.col("claim_amount") - F.col("approved_amount")).alias("total_outstanding_amount"),
    F.avg("claim_amount").alias("average_claim_amount"),
    F.max("claim_amount").alias("maximum_claim_amount"),
    F.min("claim_amount").alias("minimum_claim_amount")
)
print("Overall Claim Metrics:")
claim_metrics.show()

#Group claims by claim_type and calculate metrics:
claims_grouped_metrics = claims.groupBy("claim_type").agg(
    F.count("claim_id").alias("total_claims"),
    F.sum("claim_amount").alias("total_claim_amount"),
    F.sum("approved_amount").alias("total_approved_amount"),
    F.avg("claim_amount").alias("average_claim_amount")
)
print("Claims Grouped by Type:")
claims_grouped_metrics.show()

#Group claims by fraud_flag and calculate metrics:
claims_fraud_metrics = claims.groupBy("fraud_flag").agg(
    F.count("claim_id").alias("total_claims"),
    F.sum("claim_amount").alias("total_claim_amount"),
    F.sum("approved_amount").alias("total_approved_amount")
)
print("Claims Grouped by Fraud Flag:")
claims_fraud_metrics.show()

# Find the total claim amount and total approved amount for fraud-flagged claims only.
fraud_claims_metrics = claims.filter(claims.fraud_flag == 1).agg(
    F.sum("claim_amount").alias("total_claim_amount"),
    F.sum("approved_amount").alias("total_approved_amount")
)
print("Fraud-Flagged Claims Metrics:")
fraud_claims_metrics.show()

# Group claims by policy_id and identify policies having more than one claim.
policy_claims_metrics = claims.groupBy("policy_id").agg(
    F.count("claim_id").alias("total_claims")
).filter(F.col("total_claims") > 1)

print("Policies with More Than One Claim:")
policy_claims_metrics.show()
