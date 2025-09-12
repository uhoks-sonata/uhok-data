FROM apache/airflow:3.0.6

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY ./dags /opt/airflow/dags
COPY requirements.txt ./requirements.txt
COPY ./ETL ./ETL
# COPY ./.env ./.env
RUN pip3 install -r requirements.txt
RUN pip install ETL
RUN playwright install chromium chromium-headless-shell

USER root
RUN sudo playwright install-deps

WORKDIR /opt/uhok_data
ENV PYTHONPATH=/opt/uhok_data