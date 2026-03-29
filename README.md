Build with this:

docker compose up -d --build

go into airflow at localhost:8080 and trigger a manual dag. u should see the data written into the bronze/silver/gold folders.