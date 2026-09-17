import os
import warnings
from pyspark.sql import functions as F
 
os.environ["HADOOP_HOME"] = r"C:\Users\srilakshmi.g\hadoop"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\srilakshmi.g\AppData\Local\Programs\Python\Python36\python.exe"
 
warnings.filterwarnings("ignore")
 
from pyspark.sql import SparkSession
# creating sparkcontext object
spark = SparkSession.builder.appName("VIMS").master("local[1]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
 
policies = (spark.read
             .option("header",True)
             .option("inferSchema",True)
             .csv("data/policies.csv"))
from pyspark.sql.functions import(sum, avg, min, max, count, col,desc)
 
#Total Premium collected
 
policies.select(
    sum("premium_amount").alias("Total_Premium_Amount")
).show()
 
 
#Avg Premium collected
 
policies.select(
    avg("premium_amount").alias("Average_Premium_Amount")
).show()
 
 
#Minimum/Max Premium collected
 
policies.select(
    min("premium_amount").alias("Minimum_Premium_Amount")
).show()
 
 
#No. of policies
 
policies.select(
    count("*").alias("No_of_Policies")
).show()
 
policies.select(
    count("*").alias("No_of_Policies"),
         min("premium_amount").alias("Minimum_Premium_Amount"),
             avg("premium_amount").alias("Average_Premium_Amount"),
                 sum("premium_amount").alias("Total_Premium_Amount")
).show()
 
#groupby policy_type
policies.groupBy("policy_type").count().show()
 
#Total premium wrt policy_type
policies.groupBy("policy_type").sum("premium_amount").alias("Total_Premium").show()
 
#Apply multiple metrics -use agg
policy_summary = policies.groupBy("policy_type").agg(
    count("*").alias("Total Policies"),
    sum("premium_amount").alias("Sum_of_premium_amount"),
    max("premium_amount").alias("Maximum_Premium_Amount"),
    avg("premium_amount").alias("Average_Premium_Amount")
).orderBy(desc("Total Policies"))
policy_summary.show()
 
#where, having
#filtering before aggregation
#calculate summary only for active policies
#SELECT count(*),sum("premium_amount")
#FROM policies
#WHERE policy_status = "Active"
#GROUP BY policy_type
active_policy_summary = policies.filter(col("policy_status")=="Active").groupBy("policy_type").agg(
    count("*").alias("Total Policies"),
    sum("premium_amount").alias("Sum_of_premium_amount"),
    max("premium_amount").alias("Maximum_Premium_Amount"),
    avg("premium_amount").alias("Average_Premium_Amount")
).orderBy(desc("Total Policies"))
active_policy_summary.show()
 
#Which policy_type has total premium above 1,00,000?
#SELECT count(*),sum("premium_amount")
#FROM policies
#WHERE policy_status = "Active"
#GROUP BY policy_type
#HAVING sum("premium_amount") >= 100000
 
high_premium_active_policies= policies.where(col("policy_status")=="Active").groupBy("policy_type").agg(
    count("*").alias("Total Policies"),
    sum("premium_amount").alias("Sum_of_premium_amount"),
    max("premium_amount").alias("Maximum_Premium_Amount"),
    avg("premium_amount").alias("Average_Premium_Amount")
).filter(col("Sum_of_premium_amount")>=100000).orderBy(desc("Total Policies"))
high_premium_active_policies.show()
 
#Joins
policies = (spark.read
             .option("header",True)
             .option("inferSchema",True)
             .csv("data/policies.csv"))
 
claims = (spark.read
             .option("header",True)
             .option("inferSchema",True)
             .csv("data/claims.csv"))
 
policies.printSchema()
claims.printSchema()
 
#inner join
policy_claims_df = policies.join(claims,on="policy_id",how="inner")
policy_claims_df.select("policy_id","claim_id","claim_amount").show()
 
#outer join (policy that do not have any claims)
all_policy_claims_df = policies.join(claims,on="policy_id",how="left")
all_policy_claims_df.where("claim_id is null").select("policy_id","claim_id","claim_amount").show()
 
#anti join (policy that do not have any claims)
policy_without_claims_df = policies.join(claims,on="policy_id",how="left_anti")
policy_without_claims_df.show()
 