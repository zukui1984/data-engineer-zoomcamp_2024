## Spark Distributed Data Structures Overview

### DataFrame
- **User-Friendly**: Intuitive for users with extensive functions and library support.
- **Optimized**: Built on RDDs, enhancing performance through lazy evaluation and DAG optimization.
- **Structured Data**: Organizes data in rows and columns for efficient processing.

### Dataset
- **Type-Safe**: Exclusive to Java and Scala, offers limited Python support. Ideal for both structured and unstructured data.
- **Customizable**: Supports custom classes and types, using lazy evaluation for optimized execution via DAG.

### RDD (Resilient Distributed Dataset)
- **Core Abstraction**: Offers in-depth control over distributed data processing, prioritizing manual optimization.
- **Flexible**: Lazily evaluated without automatic logical plan construction, suitable for complex data processing tasks.

These structures form the backbone of Spark's efficient data processing capabilities, each tailored for specific use cases and optimization levels.

## Apache Spark Guide

## Setting Up Java and Spark on Linux/Bash
### Java installation
1. Download Java 11 using wget:
wget [download link>](https://jdk.java.net/archive/) 
2. Unzip the downloaded file:
```bash
tar xzvf openjdk-11.0.2_linux-x64_bin.tar.gz
```
3. Remove the compressed file:
```bash
rm openjdk-11.0.2_linux-x64_bin.tar.gz
```
4. Set the Java home path and update your system's PATH variable:
```bash
export JAVA_HOME="/mnt/c/Users/Anwender/Desktop/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
```
## Spark installation
1. Download Spark 3.3.2:
```bash
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
```
2. Unpack the Spark archive and remove the compressed file:
```bash
tar xzfv spark-3.3.2-bin-hadoop3.tgz
rm spark-3.3.2-bin-hadoop3.tgz
```
3. Set the Spark home path and update the PATH variable:
```bash
export SPARK_HOME="/mnt/c/Users/Anwender/Desktop/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
```
## Running Spark
1. Launch Spark shell:
```bash
spark-shell
```
2. Test with Scala commands:
```bash
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()
```
## Persistent environment setup
1. Add Java and Spark paths to ~/.bashrc for persistence:
```bash
echo 'export JAVA_HOME="/mnt/c/Users/Anwender/Desktop/spark/jdk-11.0.2"' 
echo 'export PATH="$JAVA_HOME/bin:$PATH"' 
echo 'export SPARK_HOME="/mnt/c/Users/Anwender/Desktop/spark/spark-3.3.2-bin-hadoop3"' 
echo 'export PATH="$SPARK_HOME/bin:$PATH"'
add all above into nano ~/.bashrc
then source ~/.bashrc
```
## PySpark setup
1. For PySpark, set up a new folder and configure the environment:
```bash
export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}"
```
3. Launch Jupyter Notebook on port 8888 and install py4j:
```bash
jupyter notebook --port=8888
pip install py4j
```
5. Start a Spark session in Jupyter Notebook:
```bash
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName('test').getOrCreate()
df = spark.read.option("header", "true").csv('taxi+_zone_lookup.csv')
df.show()
df.write.parquet('zones')
```
### Spark fundamentals
```scala
// Example: Creating a SparkSession
val spark = SparkSession.builder()
  .appName("Spark Example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
```
### Spark and SQL
```scala
spark.sql("""
SELECT * from trips_data LIMIT 5;
""").show()
```
```scala
spark.sql("""
SELECT 
    service_type,
    COUNT(*) as trip_count
FROM
    trips_data
GROUP BY
    service_type
""").show()
```
```scala
df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")
```
## Resilient Distributed Dataset (RDD) 
RDDs are a foundational concept in Apache Spark, designed for high-efficiency distributed data handling and fault tolerance. Key features include:

- **Parallel Processing**: Enables processing large datasets across a cluster.
- **Fault Tolerance**: Automatically recovers data on node failure, ensuring resilience.
- **Transformations and Actions**: Supports operations that transform data (lazily evaluated) and actions that trigger computations and produce results.

Ideal for scalable, fault-tolerant data processing, RDDs form the backbone of Spark's data processing capabilities.

## Create RDD
```scala
rdd = df_green \
    .select('lpep_pickup_datetime', 'PULocationID', 'total_amount') \
    .rdd
```
Using `.filter`
```scala
# selects all objects in the RDD
rdd.filter(lambda row: True).take(1)

# selects objects based on time filter
start = dataetime (year=2020, month=1, day=1)
rdd.filter(lambda row: row.lpep_pickup_datetime >= start).take(1)
```
