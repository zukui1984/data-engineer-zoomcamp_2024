## Data orchestration - Mage AI (Alternative Airflow)

## Table of Contents
- [Introduction](#introduction)
- [Setting Up the Environment - Postgres & Mage AI](#setting-up-the-environment)
- [Parametrizing Flows & Deployments](#parametrizing-flows--deployments)

## Introduction
Mage is an open-source, hybrid framework for transforming and integrating data in data engineering workflow orchestration. ✨

## Setting Up the Environment
Steps for environment setup, including Docker and Prefect installations.

Create Postgres setting
```
$ mkdir mage-zoomcamp
$ docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```
* On Mage portal io_config.yaml need to be adjust
```
  dev:
    POSTGRES_CONNECT_TIMEOUT: 10 
    POSTGRES_DBNAME: "{{ env_var('POSTGRES_DBNAME') }}"
    POSTGRES_SCHEMA: "{{ env_var('POSTGRES_SCHEMA') }}"
    POSTGRES_USER: "{{ env_var('POSTGRES_USER') }}"
    POSTGRES_PASSWORD: "{{ env_var('POSTGRES_PASSWORD') }}"
    POSTGRES_HOST: "{{ env_var('POSTGRES_HOST') }}"
    POSTGRES_PORT: "{{ env_var('POSTGRES_PORT') }}"
```

After this can start cloning the Mage repo:

```bash
git clone https://github.com/mage-ai/mage-zoomcamp.git mage-zoomcamp
```

Rename `dev.env` to simply `.env`— this will _ensure_ the file is not committed to Git by accident, since it _will_ contain credentials in the future.

After activate Docker Desktop we can start build the container
* To update latest Mage AI
```bash
docker pull mageai/mageai:latest
```

```bash
docker compose build
```

Finally, start the Docker container:

```bash
docker compose up
```

Now, navigate to http://localhost:6789 in the browser to see Mage AI

## ETL with GCP & Prefect
Guide on setting up ETL processes with GCP services, integrating with Prefect.

## Parametrizing Flows & Deployments
Instructions on how to parameterize Prefect flows for dynamic execution.

In this module, you'll learn how to use the Mage platform to author and share _magical_ data pipelines. This will all be covered in the course, but if you'd like to learn a bit more about Mage, check out our docs [here](https://docs.mage.ai/introduction/overview). 

### What just happened?

I just initialized a new mage repository. It will be present in the project under the name `magic-zoomcamp`. If I changed the varable `PROJECT_NAME` in the `.env` file, it will be named whatever you set it to.

This repository should have the following structure:

```
.
├── mage_data
│   └── magic-zoomcamp
├── magic-zoomcamp
│   ├── __pycache__
│   ├── charts
│   ├── custom
│   ├── data_exporters
│   ├── data_loaders
│   ├── dbt
│   ├── extensions
│   ├── interactions
│   ├── pipelines
│   ├── scratchpads
│   ├── transformers
│   ├── utils
│   ├── __init__.py
│   ├── io_config.yaml
│   ├── metadata.yaml
│   └── requirements.txt
├── Dockerfile
├── README.md
├── dev.env
├── docker-compose.yml
└── requirements.txt
```

# Data loader
![image](https://github.com/zukui1984/data-engineer-zoomcamp_2024/assets/71074389/baee7abb-caa4-4edb-935a-827798bdda9b)

# Data exporter
![image](https://github.com/zukui1984/data-engineer-zoomcamp_2024/assets/71074389/cee68234-f15f-4023-8ca2-28332cf198ba)

# Tree
![image](https://github.com/zukui1984/data-engineer-zoomcamp_2024/assets/71074389/6a8d0271-4894-45ed-afd6-f5c928ee3742)

## Assistance

1. [Mage Docs](https://docs.mage.ai/introduction/overview): a good place to understand Mage functionality or concepts.
2. [Mage Slack](https://www.mage.ai/chat): a good place to ask questions or get help from the Mage team.
3. [DTC Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration): a good place to get help from the community on course-specific inquireies.
4. [Mage GitHub](https://github.com/mage-ai/mage-ai): a good place to open issues or feature requests.
