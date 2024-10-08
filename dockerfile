FROM apache/airflow

USER root

# Install any additional dependencies if needed
RUN apt-get update && apt-get install -y jq

USER airflow

# Copy all files from the dags folder into the container
COPY dags /opt/airflow/dags/

# Copy requirements.txt and install dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy functions directory
COPY functions /opt/airflow/functions/
COPY .env /opt/airflow/.env

# Add the /opt/airflow directory to the Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Initialize the database and create the first user
RUN airflow db init && \
    airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email berwin.singh48@gmail.com

# Expose the web server port
EXPOSE 8080

# Start Airflow webserver and scheduler
CMD ["bash", "-c", "airflow webserver & airflow scheduler"]