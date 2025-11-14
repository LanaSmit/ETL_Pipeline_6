# Pipeline_Docker
This project loads NYC Green Taxi **Parquet** data into a **Postgres** database using Docker containers on **WSL Ubuntu**. The ingestion is handled through a Python script inside a Docker image. All sensitive or personal configuration details are managed securely through a .env file (excluded from version control).

## 1. Prerequisites

Make sure you have the following installed:

- **WSL2 (Ubuntu 22.04 / 24.04)**
- **Docker Desktop**
- **Git**
- **Python 3.9+** (optional for local testing)

Verify Docker works inside WSL:

```bash
docker --version
docker ps
```
## 2. Clone the Repository
```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
```
## 3. Create a .env File
```bash
nano .env
```

Inside the .env file paste the follwoing code:
```bash
POSTGRES_USER=lana
POSTGRES_PASSWORD=1234
POSTGRES_DB=ny_taxi

PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin

LOCAL_PATH=/home/lana/ETL_Pipeline_6
```
Save & exit.
## 4. Start Postgres + pgAdmin (Docker Compose)

Start the containers:
```bash
docker compose up -d
```
This starts:

- Postgres → port 5432
- pgAdmin → port 8080

Check running containers:
```bash
docker ps
```
## 5. Access pgAdmin

Open:
```bash
http://localhost:8080
```

Login using the credentials from your .env.

Add a new server:

- Host: pgdatabase
- Username: lana
- Password: 1234

## 6. Build the Ingestion Docker Image

Your Dockerfile builds the Python environment used for ingestion.
```bash
docker build -t parquet_ingest:001 .
```

## 7. Run the ETL Pipeline (Ingestion Container)

This container converts the Parquet file to a compressed CSV and inserts it into Postgres in chunks:
```bash
docker run -it \
  --network=etl-pipeline_default \
  parquet_ingest:001 \
  --user=lana \
  --password=1234 \
  --host=pgdatabase \
  --port=5432 \
  --db=ny_taxi \
  --table=green_trip_data \
  --parquet_path=green_tripdata_2019-01.parquet
```
Check your Docker network name (it may differ):
```bash
docker network ls
```
## 8. Validate Successful Ingestion

Inside pgAdmin, run: 
```bash
SELECT COUNT(*) FROM green_trip_data;
```
If rows appear, the ingestion succeeded.

## 9. Stopping the Environment

Stop the containers:
```bash
docker compose down
```
Stop + remove all volumes:
```bash
docker compose down -v
```
