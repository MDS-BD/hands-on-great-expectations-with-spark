FROM jupyter/pyspark-notebook:python-3.8.8

ENV PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip"
ENV PYSPARK_PYTHON="python"

COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN rm -f requirements.txt
