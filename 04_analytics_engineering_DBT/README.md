# Analytics Engineering with dbt (Data Build Tools)

## Overview
This week introduces dbt (data build tool), a powerful tool for transforming data stored in data warehouses. We cover the end-to-end process of using dbt to create analytical models from the Taxi Rides NY dataset.

## Key Concepts
- **Analytics Engineering**: The practice of using dbt for data transformation and modeling.
- **dbt Setup**: Configuring dbt for use with BigQuery or PostgreSQL.
- **Data Transformation**: Building dbt models to refine raw data into actionable insights.

## Prerequisites
- A functioning data warehouse (BigQuery or PostgreSQL).
- Completion of data pipelines from Week 3.

## Getting Started dbt with BigQuery
1. **In Git**:
  - Create a new git repository to store the dbt project
  - Do not add a .gitignore or a readme. Those files will be added by dbt.
2. **In BigQuery**:
  - Make the data available in tables
  - dbt will create the schemas needed for your project when you run the models.
3. **Follow** the dbt cloud set up [instructions](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/dbt_cloud_setup.md)
  - Set up a new dbt project
  - Connect to the git hub repo
  - Connect to BigQuery
4. On dbt project page
![image](https://github.com/zukui1984/data-engineer-zoomcamp_2024/assets/71074389/6eb111a2-14cf-4ae7-bf3e-ceaaddd5f82a)
5. **Edit the dbt_project.yml**
  - Change name 'my_new_project' to one of your choice
  - Under models change *'my_new_project'* to the chosen name
  - Delete the example under this model

# dbt Project Structure Guide

### Step 1: Organize Models
Create sub-folders under the `models` folder:
- `staging`
- `core`

### Step 2: Setup Staging Models
In the `staging` folder, add:
- `stg_green_tripdata.sql`
  - Include the config block: `{{ config(materialized='view') }}`
  - Use views for staging to access the latest data.

### Step 3: Configure Schema for BigQuery
Inside the `staging` folder, create `schema.yml`:
- Set `database` to your GCP project ID.
- Set `schema` to your BigQuery dataset schema.

### Step 4: Add a simple SELECT Statement to the stg_green_tripdata.sql model
```sql
{{ config(materialized='view') }}

select * from {{ source('staging','green_tripdata') }}
limit 100
```
### Step 5: Add to the stg_green_tripdata model
```sql

{{ config(materialized='view') }}

select
    -- identifiers
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(trip_type as integer) as trip_type,

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    cast(congestion_surcharge as numeric) as congestion_surcharge

from {{ source('staging', 'green_tripdata') }}
limit 100
```
Example of macros
```jinja
{# This macro returns the description of the payment_type #}

{% macro get_payment_type_description(payment_type) %}

    case {{ payment_type }}
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
    end

{% endmacro %}
```

### Complete project data flow
![image](https://github.com/zukui1984/data-engineer-zoomcamp_2024/assets/71074389/2bffc09b-0dd7-4fcc-bded-b92f34521645)

