FROM apache/airflow:2.8.0-python3.10

USER root

RUN apt update

RUN apt install -y default-jdk 
RUN apt install -y git 
RUN apt install -y wget

RUN wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz && tar -xvzf spark-3.3.0-bin-hadoop3.tgz

RUN mv spark-3.3.0-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

COPY . .

USER airflow

RUN pip install -r requirements.txt