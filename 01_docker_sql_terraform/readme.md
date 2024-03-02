# Docker, Postgres, and GCP with Terraform Project

This project demonstrates a comprehensive workflow from containerization with Docker, data ingestion into Postgres, to infrastructure provisioning on Google Cloud Platform (GCP) using Terraform. It includes practical exercises like ingesting New York Taxi Data into Postgres, connecting to Postgres with Jupyter and Pandas for data analysis, and setting up cloud infrastructure with Terraform.

## Table of Contents

- **Docker + Postgres**
  - [Introduction to Docker](#introduction-to-docker): Basics of containerization and initial Docker commands.
  - [Ingesting NY Taxi Data to Postgres](#ingesting-ny-taxi-data-to-postgres): Steps to load the dataset into a Postgres database using Docker.
  - [Connecting to Postgres](#connecting-to-postgres): Utilizing Jupyter notebooks and pandas for data manipulation.
  - [Dockerizing the Ingestion Script](#dockerizing-the-ingestion-script): Creating a Dockerfile to automate the data ingestion process.
  - [Running Postgres and pgAdmin with Docker-Compose](#running-postgres-and-pgadmin-with-docker-compose): Simplifying container management.
  - [SQL Refresher](#sql-refresher): Basic SQL queries for data analysis.

- **GCP + Terraform**
  - [Introduction to GCP](#introduction-to-gcp): Overview of Google Cloud Platform services.
  - [Introduction to Terraform](#introduction-to-terraform): Fundamentals of using Terraform for infrastructure as code.
  - [Workshop: Creating GCP Infrastructure with Terraform](#workshop-creating-gcp-infrastructure-with-terraform): Step-by-step guide to deploying infrastructure on GCP.

## Project Setup

### Docker and Postgres Setup:

1. Install Docker Desktop and run basic Docker commands.
2. Use Docker Compose to run Postgres and pgAdmin containers.
3. Ingest NY Taxi data into Postgres using a Docker container.

### Data Analysis with Jupyter:

- Connect to the Postgres database using Jupyter notebooks to perform data analysis.

### Infrastructure with Terraform on GCP:

- Set up a GCP project and authenticate with a service account.
- Define infrastructure with Terraform files (`main.tf` and `variables.tf`).
- Apply Terraform configuration to provision resources on GCP.

## Code Examples

### Dockerfile for Ingestion Script:

```Dockerfile
FROM python:3.9
RUN pip install pandas psycopg2-binary
COPY ingestion_script.py /app/ingestion_script.py
CMD ["python", "/app/ingestion_script.py"]
````


### Terraform Configuration for GCP Bucket:
```
resource "google_storage_bucket" "data-lake-bucket" {
  name     = "ny-taxi-data-lake-${var.project_id}"
  location = "US"
}
```
### Useful Commands
- Docker: ```docker build . -t ingestion-container```
- Terraform: ```terraform apply```

## Conclusion
This project encapsulates a real-world scenario of data engineering tasks including data ingestion, database management, and cloud infrastructure provisioning, showcasing the integration between Docker, Postgres, and GCP with Terraform.
