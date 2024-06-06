## Starting Airflow with Docker & Running it from Local Web UI

Make sure you are in the airflow directory
Make sure Docker is running on your local computer

To Build and import requirements.txt:
`docker-compose up -d`
`docker-compose build`

To Enter Container and edit any files:
`docker exec -it airflow-airflow-webserver-1 bash`

If neede, to replace DAG without ripping down container/images while inside container:
`docker cp dags/databricks_etl_dag.py airflow-airflow-webserver-1:/opt/airflow/dags`

If needed:
`docker restart airflow-airflow-webserver-1`


You can fetch the latest docker-compose.yml @
- https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml


Helpful resources:
- https://stackoverflow.com/questions/32378494/how-to-run-airflow-on-windows


