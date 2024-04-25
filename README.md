# ODS-ANALYTICS-SYNC
This repository contains an Apache Airflow DAG to synchronize the ODS (Opendatasoft) monitoring tables with a local database. This is usefull if the usage of the Opendatasoft-Portal should be analyzed and backtraceable beyond the timerange limit of the ODS-contract.

## Getting Started

### Project Setup
- **Setting Port**: Start an Aiflow service e.g. by using a Docker image the current DAG runs with the image apache/airflow:2.8.1 and postgres:14-alpine. Also set up a (postgres) database to store the tables and install the python libraries in the requirements file. 

- **Environment Variables**: Rename `.env-example` to `.env` and set the parameters.

- **Hardcoded Links**: ogd_web_analytics_utilities.py contains two constants `USER_DATA_API_URL` and `DATASETS_DATA_API_URL` that are hardcoded to point towards the monitoring endpoints of data.bl.ch. They have to be switched with the correct urls for your portal.


## DAGs

### OGD Web Analytics ETL
> ogd_web_analytics_etl

The DAG is documented inline using md docs. Utility functions are loaded from ogd_web_analytics_utilities.py.

The tables that need to exist are the following two, where user_actions contains data from the endpoint [portal]/api/explore/v2.1/monitoring/datasets/ods-api-monitoring and datasets contains data from the endpoint [portal]/api/explore/v2.1/monitoring/datasets/ods-datasets-monitoring

```sql
table ogd_analytics.user_actions (
    timestamp TIMESTAMP,
    user_ip_addr VARCHAR,
    user_id VARCHAR,
    dataset_id VARCHAR,
    api_type VARCHAR,
    mobile VARCHAR,
    action VARCHAR,
    attributes VARCHAR,
    filename VARCHAR,
    query_text VARCHAR
);
```
```sql
table ogd_analytics.datasets (
    timestamp TIMESTAMP,
    dataset_id VARCHAR,
    title VARCHAR,
    modified VARCHAR,
    publisher VARCHAR,
    license VARCHAR,
    keyword VARCHAR,
    theme VARCHAR,
    api_call_count INTEGER,
    download_count INTEGER,
    records_count INTEGER,
    visibility VARCHAR
);
```
It is advisable to cleanse the monitoring data before publishing as bots are not filtered out automatically and internal users may should be omitted. data.bl.ch uses the following two views to cleanse the raw data in the above tables. The views filter out usage from bots and logged in users, and creates a continuous date series with missing values if for some days no data is available.

```sql
CREATE OR REPLACE VIEW ogd_analytics.daily_external_user_dataset_interactions
AS WITH date_range AS (
         SELECT generate_series(min(date_trunc('day'::text, user_actions."timestamp")), max(date_trunc('day'::text, user_actions."timestamp")), '1 day'::interval) AS date
           FROM ogd_analytics.user_actions
        ), interaction_counts AS (
         SELECT date_trunc('day'::text, user_actions."timestamp") AS date,
            count(user_actions."timestamp") AS dataset_interactions
           FROM ogd_analytics.user_actions
          WHERE user_actions.user_id::text = 'anonymous'::text AND user_actions.dataset_id::text !~~ '%NULL%'::text AND (user_actions.user_agent IS NULL OR user_actions.user_agent !~~* '%bot%'::text)
          GROUP BY (date_trunc('day'::text, user_actions."timestamp"))
        )
 SELECT dr.date,
    ac.dataset_interactions
   FROM date_range dr
     LEFT JOIN interaction_counts ac ON dr.date = ac.date
  ORDER BY dr.date;
```

```sql
CREATE OR REPLACE VIEW ogd_analytics.daily_unique_external_user_ip_count
AS WITH date_range AS (
         SELECT generate_series(min(date_trunc('day'::text, user_actions."timestamp")), max(date_trunc('day'::text, user_actions."timestamp")), '1 day'::interval) AS date
           FROM ogd_analytics.user_actions
        ), action_counts AS (
         SELECT date_trunc('day'::text, user_actions."timestamp") AS date,
            count(DISTINCT user_actions.user_ip_addr) AS unique_ip_count
           FROM ogd_analytics.user_actions
          WHERE user_actions.user_id::text = 'anonymous'::text AND (user_actions.user_agent IS NULL OR user_actions.user_agent !~~* '%bot%'::text)
          GROUP BY (date_trunc('day'::text, user_actions."timestamp"))
        )
 SELECT dr.date,
    ac.unique_ip_count
   FROM date_range dr
     LEFT JOIN action_counts ac ON dr.date = ac.date
  ORDER BY dr.date;
```

The sql script in dags>ogd>sql shows how the views can be accessed for synchronization with the portal.

## Contact
If you have troubles or questions please don't hesitate to contact us at ogd@bl.ch