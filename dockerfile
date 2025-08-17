FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow
COPY requirements.txt /requirements.txt
COPY .env /.env
RUN pip install uv
RUN uv pip install googlemaps anyascii openmeteo_requests requests_cache retry_requests pytile asyncio aiohttp dotenv