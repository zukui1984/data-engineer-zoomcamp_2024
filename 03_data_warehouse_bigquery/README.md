# Data Warehousing with BigQuery

## Quick Overview
Dive into data warehousing fundamentals, contrasting OLAP/OLTP, and spotlighting Google BigQuery for scalable analytics.

## BigQuery Highlights
- **Serverless & Scalable**: Ideal for large datasets.
- **Cost-Effective**: Pay for what you use, no upfront costs.
- **Fully Integrated**: Works seamlessly with Google Cloud services.
- **Secure**: Comprehensive encryption and access control.

## Essentials
- **Understand Billing**: Know how storage and queries impact costs.
- **Data Formats Matter**: Optimize query performance with the right format.
- **Optimize with Partitioning/Clustering**: Key for efficient data querying.
- **Efficient Data Transfer**: Use tools for seamless data loading.

## Process that happening around GCP
* Creating external table
```SQL
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
);
```

* Creating and selecting partitioned table
```SQL
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;
```
```SQL
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';
```




