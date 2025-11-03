FROM python:3.9

WORKDIR /app

RUN pip install pandas sqlalchemy psycopg2 pyarrow
COPY green_tripdata_2019-01.parquet green_tripdata_2019-01.parquet
COPY ingesting.py ingesting.py

ENTRYPOINT ["python", "ingesting.py"]


# docker run -it \
#   -e POSTGRES_USER="lana" \
#   -e POSTGRES_PASSWORD="1234" \
#   -e POSTGRES_DB="ny_taxi" \
#   -v /home/lana/ETL_Pipeline_6/postgres_data:/var/lib/postgresql/data:rw \
#   -p 5432:5432 \
#   --network=pg-network \
#   --name=pg-database \
#   postgres:13

# docker run -it \
#   --network=pg-network \
#   python_test:001 \
#   --user=lana \
#   --password=1234 \
#   --host=pg-database \
#   --port=5432 \
#   --db=ny_taxi \
#   --table=green_trip_data \
#   --parquet_path=green_tripdata_2019-01.parquet