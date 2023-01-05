FROM python:3.8-slim
LABEL maintainer="Sazz"

ARG AIRFLOW_VERSION=2.0.2
ARG AIRFLOW_HOME=/opt/airflow

ENV AIRFLOW_HOME=${AIRFLOW_HOME}

RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    locales \
    && apt-get clean

COPY ./constraints-3.8.txt /constraints-3.8.txt
COPY ./requirements.txt /requirements.txt

RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    pip install apache-airflow[postgres]==${AIRFLOW_VERSION} --constraint /constraints-3.8.txt &&\
    pip install -r requirements.txt



COPY ./entrypoint.sh ./entrypoint.sh

RUN chmod +x ./entrypoint.sh
RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow

WORKDIR ${AIRFLOW_HOME}

RUN mkdir dags

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
   