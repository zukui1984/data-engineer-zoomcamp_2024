import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .master("spark://zukui-1984:7077") \
    .appName('yellow_green') \
    .getOrCreate()
    


# Read the green taxi data
df_green = spark.read.parquet('data/pq/green/*/*')

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

# Read the yellow taxi data
df_yellow = spark.read.parquet('data/pq/yellow/*/*')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# Define common columns to select
common_colums = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]

# Select common columns and add a service_type column
df_green_sel = df_green.select(common_colums).withColumn('service_type', F.lit('green'))
df_yellow_sel = df_yellow.select(common_colums).withColumn('service_type', F.lit('yellow'))

# Union the green and yellow taxi data
df_trips_data = df_green_sel.unionAll(df_yellow_sel)

# Register a temporary view to use SQL queries
df_trips_data.createOrReplaceTempView('trips_data')

# SQL query to aggregate data
df_result = spark.sql("""
SELECT 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

# Write the result to a parquet file
df_result.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite')