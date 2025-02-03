
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col, lit, current_date, concat_ws
from pyspark.sql import SparkSession

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dyf_yellow_tripdata = glueContext.create_dynamic_frame.from_catalog(database='nyctaxi_db', table_name='yellow_tripdata')
df_yellow_tripdata = dyf_yellow_tripdata.toDF()

dyf_taxi_zone_lookup_csv = glueContext.create_dynamic_frame.from_catalog(database='nyctaxi_db', table_name='taxi_zone_lookup_csv')
df_taxi_zone_lookup_csv = dyf_taxi_zone_lookup_csv.toDF()

df_yellow_tripdata_after_dropped_null_values = df_yellow_tripdata.dropna(subset=["vendorid", "payment_type","passenger_count", "ratecodeid"])
#df_yellow_tripdata_after_dropped_null_values.show(3)

# create temp views

df_yellow_tripdata_after_dropped_null_values.createOrReplaceTempView("yellow_tripdata")
df_taxi_zone_lookup_csv.createOrReplaceTempView("taxi_zone_lookup")

glueContext.sql("""
SELECT * 
FROM yellow_tripdata
LIMIT 10
""").show()
glueContext.sql("""
SELECT * 
FROM taxi_zone_lookup
LIMIT 10
""").show()
#pickup zone join
df_yellow_trip_pkcup =spark.sql("""
select YT.*,PLKP.borough as PKUP_borough, PLKP.zone as PKUP_zone,  PLKP.service_zone as PKUP_service_zone from yellow_tripdata YT, taxi_zone_lookup PLKP
where YT.pulocationid=PLKP.locationid
""")

df_yellow_trip_pkcup.createOrReplaceTempView("df_yellow_trip_pkcup")
df_yellow_trip_pkcup.show(3)
#select YT.*,PLKP.borough as PKUP_borough, PLKP.zone as PKUP_zone,  PLKP.service_zone as PKUP_service_zone from yellow_tripdata YT, taxi_zone_lookup PLKP
#where YT.pulocationid=PLKP.locationid


#drop zone join
df_yellow_trip_drop_pckup=spark.sql("""
select YT.*,DROP.borough as DROP_borough, DROP.zone as DROP_zone,  DROP.service_zone as DROP_service_zone from df_yellow_trip_pkcup YT, taxi_zone_lookup DROP
where YT.dolocationid=DROP.locationid
""")

df_yellow_trip_drop_pckup.show(3)
# Specify the S3 path where you want to save the Parquet file
#s3_final_output = "s3://serverlessanalytics-682033496527-transformed/"

df_yellow_trip_drop_pckup.show(3)
# Save the DataFrame as a Parquet file

df_yellow_trip_drop_pckup.write.mode("overwrite").parquet("s3://serverlessanalytics-682033496527-transformed/nyc-taxi/") 


job.commit()