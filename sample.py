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
 
#create a calculated column outstanding amount
 
policies_outstanding_amount_df = policies.withColumn("outstanding_amount", F.col("sum_insured")-F.col("premium_amount"))
 
policies_outstanding_amount_df.select("policy_id","sum_insured","premium_amount","outstanding_amount").show()
 
#applying constant values
from pyspark.sql.functions import lit
 
policies_with_source = policies_outstanding_amount_df.withColumn("DataSource",lit("VIMS_DB"))
policies_with_source.select("policy_id","sum_insured","premium_amount","outstanding_amount","DataSource").show()
 
#CASE & WHEN
from pyspark.sql.functions import when
 
policies_with_categories_df = policies_with_source.withColumn("policy_category",                                                   when(F.col("premium_amount")>=40000,"Gold Policy")
                                                              .when(F.col("premium_amount")>=20000,"Silver Policy")
                                                              .otherwise("Bronze Policy"))
policies_with_categories_df. select("policy_id","sum_insured","premium_amount","outstanding_amount","DataSource","policy_category").show()
 
#drop existing columns
policies_with_categories_df.drop("DataSource").show()
 
#sorting records
policies_with_categories_df.select("policy_id","sum_insured","premium_amount").orderBy(F.col("premium_amount").desc()).show();
policies_with_categories_df.select("policy_id","sum_insured","premium_amount","outstanding_amount").orderBy(F.col("premium_amount").desc(), F.col("outstanding_amount").asc()).show()
 
#Removing Duplication
policies_with_categories_df.select("policy_id").distinct().show()
 
#using dropDuplicates
unique_policies_df = policies_with_categories_df.drop_duplicates(["policy_id"])
unique_policies_df.show()