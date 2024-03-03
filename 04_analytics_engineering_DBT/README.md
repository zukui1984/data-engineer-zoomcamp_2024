# Google Cloud and DBT Project Setup

## Google Cloud Setup
1. **Create/Select GCP Project**: Choose an existing project or set up a new one in Google Cloud Platform (GCP).
2. **Enable APIs**: Make sure the BigQuery API is enabled.
3. **Service Account**: Create a service account with BigQuery Admin permissions and download the JSON key.

## GitHub Repository Setup
- **Create Repo**: Initialize an empty repository on GitHub for your project.

## DBT Cloud Account
1. **Sign Up**: Register for a free developer account on [dbt Cloud](https://cloud.getdbt.com/).
2. **Create Project**: Follow the prompts to set up your project, selecting BigQuery as your database.
3. **Load JSON Key**: Upload your BigQuery service account JSON key. Use 'EU' for location if you're in Europe to avoid region conflicts.

## Starting Your Project
- **Initialize Project**: In dbt Cloud, initialize your project (similar to `dbt init` in CLI).
- **Branching**: Remember to create branches for working on new features or updates.

### Tips:
- Utilize the dbt Cloud GUI console for executing commands.
- Ensure your dbt Cloud project is linked to your GitHub repo for seamless version control.

For more detailed instructions, refer to the [dbt documentation](https://docs.getdbt.com/).

