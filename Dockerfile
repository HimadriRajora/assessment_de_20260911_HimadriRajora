# One image for both services: Airflow and JupyterLab run the same pipeline
# code, so they must run the same dependency set.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PROJECT_DIR=/opt/project \
    AIRFLOW_HOME=/opt/project/airflow_home \
    PYTHONPATH=/opt/project

# build-essential is needed by a couple of wheels-less transitive deps;
# curl is there so the compose healthchecks have something to call with.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

EXPOSE 8080 8888
