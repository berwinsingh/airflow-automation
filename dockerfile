FROM apache/airflow

USER root

# Install any additional dependencies if needed
# RUN apt-get update && apt-get install -y <package-name>

USER airflow

# Copy the DAG file into the container
COPY dags/test_workflow.py /opt/airflow/dags/

# Initialize the database and create the first user
RUN airflow db init && \
    airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Expose the web server port
EXPOSE 8080

# Start Airflow webserver and scheduler
CMD ["bash", "-c", "airflow webserver & airflow scheduler"]