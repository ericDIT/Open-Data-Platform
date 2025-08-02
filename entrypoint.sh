#!/bin/bash

export PYTHONPATH="/opt/airflow/include:/opt/airflow/dags"

echo "Initialize Airflow..."
airflow db init
echo "Airflow db initialized"

if airflow users list | awk '{print $1}' | grep -q "${AIRFLOW_ADMIN_USERNAME:-standard_admin}"; then 
    echo "User '${AIRFLOW_ADMIN_USERNAME:-standard_admin}' already exists, skipping creation... "
else 
    echo "Creating new Admin-User... "
    airflow users create \
        --username "${AIRFLOW_ADMIN_USERNAME:-standard_admin}" \
        --password "${AIRFLOW_ADMIN_PASSWORD:-standard_admin}" \
        --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
        --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
        --role Admin \
        --email "${AIRFLOW_ADMIN_EMAIL:-admin@test.com}"
    echo "Admin-User created!"
fi

echo "Starting Airflow webserver..."
airflow webserver &
echo "Starting Airflow scheduler..."
exec airflow scheduler
