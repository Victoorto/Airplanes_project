import pyspark.sql.functions as f
from pyspark.sql import SparkSession

#Start the Spark session
spark =  SparkSession.builder.appName('Airlines_project')\
         .master("local").config("dfs.client.read.shortcircuit.skip.checksum", "true").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

#Load the data into a DataFrame (Bronze Table)
df_spark = spark.read.csv(r'C:\Users\victo\Documents\Portfolio\Airlines_project\Data\raw\datasets\rohitgrewal\airlines-flights-data\versions\1\airlines_flights_data.csv', header=True, inferSchema=True)

#Initial exploration, numbers of columns and rows, the Schema and the summary
print("Number of rows and columns:", (df_spark.count(), len(df_spark.columns)))
df_spark.printSchema()
df_spark.describe().show(truncate=False)

#Unique values of key columns
for col_name in ['airline', 'class','departure_time', 'arrival_time', 'stops']:
    print(f"{col_name} unique values:")
    df_spark.select(col_name).distinct().show()

#count the duplicate values
duplicate_count  = df_spark.groupBy(df_spark.columns).count().where(f.col('count') > 1).count()
print(f"Number of duplicate values: {duplicate_count}")

#Check for null values on each column
total_rows = df_spark.count()
null_report = df_spark.select([
                               (f.count(f.when(f.col(c).isNull(), c)) / total_rows * 100).alias(c)
                               for c in df_spark.columns
                              ])

null_report.show(truncate=False)

#Outiliers of the duration (since no single flight can surpasse 20 Hours)
df_spark.sort(f.col('duration').desc()).show(5, truncate=False)

#Create a copy of the original DF to do cleaning
df_cleaning = df_spark.select("*")

#Remove the '_' from the columns airline, departure time and arrival time
replace_columns = ['airline', 'departure_time', 'arrival_time', 'stops']

for col_name in replace_columns:
    df_cleaning = df_cleaning.withColumn(col_name, f.regexp_replace(col_name, '_', ' '))

#Since no nulls nor duplicates were founded it we can save the df in a clean way using dropDuplicates() and na.drop()
df_clean = df_cleaning.dropDuplicates().na.drop()


#Save the data for further use (Since the data did not have mayor changes we can catalogue the Bronze table equal to the Silver Table)
output_path = r'C:\Users\victo\Documents\Portfolio\Airlines_project\Data\clean'
df_clean.write.format('parquet').mode('overwrite').save(output_path)

