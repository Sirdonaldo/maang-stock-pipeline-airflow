# MAANG Stock Price ETL with Airflow, Trino, and Iceberg
# maang-stock-pipeline-airflow
Apache Airflow DAG for ingesting MAANG stock data from Polygon API into Iceberg with Trino.
This project was created as part of the DataExpert.io Data Engineering Bootcamp.  
It defines an Apache Airflow DAG (`starter_dag.py`) that ingests daily MAANG stock prices from the Polygon API, stages them in Iceberg tables, runs data quality checks, and maintains a rolling 7-day cumulative table.

## Features
- **Airflow DAG** orchestrates daily tasks
- **Polygon API** provides MAANG stock price data
- **Iceberg + Trino** store and query partitioned tables
- **Data Quality Checks** ensure data consistency
- **7-Day Cumulative Table** for trend analysis

## DAG Flow
1. Create production table  
2. Create staging table  
3. Load daily data from Polygon API  
4. Run data quality checks  
5. Exchange staging data into production  
6. Drop staging table  
7. Maintain rolling 7-day cumulative data  

## Files
- `starter_dag.py` – Airflow DAG definition
- `requirements.txt` – Python dependencies (optional)

## Setup and How to Run
1. Clone this repo  
2. Place `starter_dag.py` in your Airflow `dags/` folder  
3. Configure Airflow Variables for:
   - `TABULAR_CREDENTIAL`
   - `POLYGON_CREDENTIALS`
4. Trigger the DAG in Airflow UI  

---

## 4. Commit and Push
- In your terminal:  
  ```bash
  git init
  git add .
  git commit -m "Initial commit: starter_dag for MAANG stock pipeline"
  git branch -M main
  git remote add origin https://github.com/<your-username>/<repo-name>.git
  git push -u origin main
