# Project: Data Pipelines

## 1. Project Template

### DAGs
- [dwh.cfg](dags\dwh.cfg): Configuration file for the Songplays DAG.
- [main_dag.py](dags\main_dag.py): Main DAG file for the project.

### Plugins

##### Helpers
- [common_queries.py](plugins\helpers\common_queries.py): Contains common queries like COPY, etc.
- [songplays_queries.py](plugins\helpers\songplays_queries.py): Main file containing query support for the main DAG.

##### Operators
- [create_table.py](plugins\operators\create_table.py): Operator to create a table in Redshift.
- [data_quality.py](plugins\operators\data_quality.py): Operator to check the data quality of tables in Redshift.
- [load_dimension.py](plugins\operators\load_dimension.py): Operator to load dimension tables into Redshift.
- [load_fact.py](plugins\operators\load_fact.py): Operator to load fact tables into Redshift.
- [stage_redshift.py](plugins\operators\stage_redshift.py): Operator to copy data from an S3 bucket to Redshift tables.

## 2. How to Run

### Step 1: Build Airflow Container
- Run the following command to build the Airflow container:

```docker compose up```

### Step 2: Login to Airflow
- Access Airflow using a web browser:
- URL: http://localhost:8080/
- Username: airflow
- Password: airflow

### Step 3: Set Connections and Variables in Airflow UI

### Step 4: Run DAG
Run the DAG from the Airflow web interface to start the data pipeline.

### Step 5 (optional): Remove container 
```docker compose down```
# Result

<figure>
  <img src="image\airflow_result1.png" alt="airflow_result1" width=100% height=100%>
</figure>

<figure>
  <img src="image\airflow_result2.png" alt="airflow_result1" width=100% height=100%>
</figure>

<figure>
  <img src="image\redshift_result.png" alt="redshift_result" width=100% height=100%>
</figure>