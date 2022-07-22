#!/bin/bash 
airflow connections add 'postgres_optionsdata' \
    --conn-type 'postgres' \
    --conn-login '$APP_DB_USER' \
    --conn-password '$APP_DB_PASS' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-schema '$APP_DB_NAME' \