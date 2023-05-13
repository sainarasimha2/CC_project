from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import sum, col, desc

spark = SparkSession.builder \
    .appName("Read CSV file using Spark") \
    .getOrCreate()
    
# Read the data

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("gs://traffic-data-set/Traffic_Counts_at_Signals2.csv")

# Process the data


# Filter the rows where PED_VALUE is greater than 100
filtered_data = df.filter(df.PED_VALUE > 1000)

# Select only the LOCATION,and PED_VALUE columns
selected_data = filtered_data.select("LOCATION", "PED_VALUE")

# Select the location_id and veh_value columns, and sort by veh_value in descending order
veh_counts_df = df.select("LOCATION", "VEH_VALUE").orderBy(desc("VEH_VALUE"))

# Take the top 5 rows
top_5_locations_df = veh_counts_df.limit(5)
                                
# Group the data by location_id and calculate the sum of ped_value and veh_value                          
grouped_counts_df = df.groupBy("LOCATION").agg(sum("PED_VALUE").alias("ped_total"), sum("VEH_VALUE").alias("veh_total"))

# Calculate the ratio of ped_value to veh_value for each location
ped_veh_ratio_df = grouped_counts_df.withColumn("ped_veh_ratio", grouped_counts_df["ped_total"] / grouped_counts_df["veh_total"]).select("LOCATION", "ped_veh_ratio")

  
# Write the output

filtered_data.write.format("parquet") \
    .mode("overwrite") \
    .save("gs://traffic-data-set/filtered-data/traffic-data")
selected_data.write.format("parquet") \
    .mode("overwrite") \
    .save("gs://traffic-data-set/filtered-data/dataSet")
ped_veh_ratio_df.write.format("parquet") \
    .mode("overwrite") \
    .save("gs://traffic-data-set/filtered-data/ratioSet")
top_5_locations_df.write.format("parquet") \
    .mode("overwrite") \
    .save("gs://traffic-data-set/filtered-data/topSet")

spark.stop()
