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
## Connecting Google Cloud Storage
1. **Create GCP Bucket:** Note the bucket name `dtc_data_lake_nytaxi_zukui`
2. **Setup gsutil:** Use `gsutil -m cp -r  pq/ gs://dtc_data_lake_nytaxi_zukui/pq `.
3. **Create lib Folder & Install gsutil:** 
   - `mkdir lib`
   - `gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar lib/`
4. **IAM Permissions:** Ensure the service account has `Storage Object Viewer` role.
5. **Bucket Permissions:** Verify permissions in GCP Console > Storage > Browser.

## Creating Local Spark Cluster
1. **Setup Spark:** Navigate to your Spark folder.
2. **Start Spark Master & Slave:**
   - Start Master: `./sbin/start-master.sh`
   - View at `localhost:8080`.
   - Start Slave: `./sbin/start-slave.sh spark://zukui-1984:7077`
3. **Jupyter & PySpark:**
   - Convert Jupyter to Python: `jupyter nbconvert --to=script 10_spark_duplicate_06.ipynb`
   - Setup `PYTHONPATH` for PySpark.
       * export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}"
       * export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}" 
   - Run Python Script: `python3 10_spark_duplicate_06.py`

## Dataproc Cluster for Reports 2020 & 2021
1. **Enable Dataproc in GCP Marketplace**
2. **Submit Job in Dataproc:** Use `gsutil cp` for transferring scripts.
   `gsutil cp 11_spark_duplicate_parser.py gs://dtc_data_lake_nytaxi_zukui/jnotebook/11_spark_duplicate_parser.py`
3. **Use Dataproc to Create Reports:** Provide necessary arguments.
   `--input_green=gs://dtc_data_lake_nytaxi_zukui/pq/green/2021/*`
   `--input_yellow=gs://dtc_data_lake_nytaxi_zukui/pq/yellow/2021/*` 
   `--output=gs://dtc_data_lake_nytaxi_zukui/report-2021`
4. **Go to IAM to add "Dataproc Administrator**
5. **Copy & paste Dataproc Documentation**
  gcloud dataproc jobs submit pyspark \
```bash
     --cluster=dtc-spark-cluster \
    --region=us-central1 \
  gs://dtc_data_lake_nytaxi_zukui/jnotebook/11_spark_duplicate_parser.py \
    -- \
    --input_green=gs://dtc_data_lake_nytaxi_zukui/pq/green/2020/* \
    --input_yellow=gs://dtc_data_lake_nytaxi_zukui/pq/yellow/2020/* \
    --output=gs://dtc_data_lake_nytaxi_zukui/report-2020
 ```
## Connecting Spark to BigQuery
1. **Upload Script to GCS:** `gsutil cp 12_spark_bigquery.py gs://dtc_data_lake_nytaxi_zukui/jnotebook/12_spark_bigquery.py`
2. **Submit Pyspark Job with BigQuery Connector:**
```bash
   gcloud dataproc jobs submit pyspark \
     --cluster=dtc-spark-cluster \
     --region=us-central1 \
     --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
   gs://dtc_data_lake_nytaxi_zukui/jnotebook/12_spark_bigquery.py \
     -- \
     --input_green=gs://dtc_data_lake_nytaxi_zukui/pq/green/2020/* \
     --input_yellow=gs://dtc_data_lake_nytaxi_zukui/pq/yellow/2020/* \
     --output=trips_data_all.report-2020
 ```
3. **The data will uploaded on BigQuery `trips_all_data`**

