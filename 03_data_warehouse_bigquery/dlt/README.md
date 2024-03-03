# Data Ingestion Workshop - dlt (data load tools) 

This workshop covers the essentials of data ingestion processes, focusing on practical techniques and tools for efficiently moving data into a centralized data warehouse. Key topics include:

- Overview of data ingestion fundamentals.
  * Data ingestion: Process extracting data from a producer , transporting it to a convenient environment & preparing it for usage by normalizing it (possible cleaning & adding metadata)
  * **Structured format** with explicit schema: Parquet, Avro or table in DB
  * **Unstructured format** without explicit schema: CSV, JSON
- Step-by-step guidance on setting up data pipelines.


## Getting Started

1. **Setup**: Prepare your environment with the necessary software and tools.
```bash
# Make sure you are using Python 3.8-3.11 and have pip installed
# spin up a venv
python -m venv ./env
source ./env/bin/activate
# pip install
pip install dlt[duckdb]
```
2. Grab your data from above and run this snippet
* We define a pipeline, which is a connection to a destination
* We run the pipeline, printing the outcome
```bash
# define the connection to load to. 
# We now use duckdb, but you can switch to Bigquery later
pipeline = dlt.pipeline(pipeline_name="taxi_data",
						destination='duckdb', 
						dataset_name='taxi_rides')

# run the pipeline with default settings, and capture the outcome
info = pipeline.run(data, 
                    table_name="users", 
                    write_disposition="replace")

# show the outcome
print(info)
```
If you are running dlt locally you can use the built in streamlit app by running the cli command with the pipeline name we chose above.
```bash
dlt pipeline taxi_data show
```
For more example, please see this [link](https://colab.research.google.com/drive/1kLyD3AL-tYf_HqCXYnA3ZLwHGpzbLmoj#scrollTo=LgI8VPNCrdGi&forceEdit=true&sandboxMode=true) 

Documention - [dlthub.com](https://dlthub.com/)
