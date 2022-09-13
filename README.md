# OPEN_OPTIONS
### *A Data Pipeline Solution for Collecting Options Data at Scale*
### *Aim of the project is to be able to run the script once and have list of stocks being constantly fetched on regular intervals and the data thats fetched using the API should be able to convert to csv file for further data analytics and for improving prediction processes.*

## Installation
If you have Git installed, clone the repository to your local machine.

```bash
git clone https://github.com/seepala98/open_options.git
```

- `dags`: Contains the DAGs you'll need for
- `csv_file`: Contains the csv file of the stocks that are created
- `logs`: Contains logs from your Airflow instance (added this to .gitignore so we wont push all logs here)
- `pgdata`: Contains data from Postgres (this folder would be created on the docker container not on the local repo)
- `source`: Contains a script to initialise Airflow
- `docker-compose.yml`: File that defines the services, networks, and volumes for our app (get the official docker-compose.yml from https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html and do the necessary changes)

Not all the services under the docker-compose are required. This project used only the absolute needed services to be present in the docker-compose file.

Then, change the required variables in the `.env` file. Note that when you edit the `POSTGRES_*` variables, you must change the associated variables in `AIRFLOW__CORE__SQL_ALCHEMY_CONN`. 
input your TD Ameritrade (TDA) API key : u can get the api key by registering the app at https://developer.tdameritrade.com/apis  

## Initialisation
Next, we'll initialise Airflow with the command below. This will:

1. Start a PostgreSQL container
2. Initialise the Airflow Metadata Database and create an Airflow account

```bash
docker-compose up airflow-init
```

The `airflow-init` container will shut down after the initialisation is complete. The `postgres` container will continue running. You can stop it with `docker-compose stop` or `docker stop <container id>` if you don't need it.

## Usage

### Launch
Launch Airflow with the command below. This will:

1. Start a PostgreSQL container if it's not already running
2. Start the Airflow Scheduler container
3. Start the Airflow Webserver container

```bash
docker-compose up
```

In a browser, navigate to [http://localhost:8088](http://localhost:8088) to access the Airflow UI. For the first time login, use the credentials `APP_AIRFLOW_USERNAME` and
`APP_AIRFLOW_PASSWORD` which were set as part of the docker-compose.yaml file.

### Shutdown
Use the following command:

```bash
docker-compose stop
```

### Extracting Data

#### Export to CSV
You can extract the data in CSV format via the command line using the commands below:

```bash
# Check Postgres container ID
docker ps

# Go to folder to store files
cd <directory-to-store-csv-files>

# Extract CSV file
docker exec -t <first-few-characters-of-docker-container> psql -U airflow -d optionsdata -c "COPY table_name to STDOUT WITH CSV HEADER" > "csv_file/filename.csv"
```

### Full Reset 
To restart the environment:

```bash
# Completely shut down the app
docker-compose down

# Remove contents of mounted volumes
rm -rf logs/*
rm -rf pgdata/*
```