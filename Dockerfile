FROM jupyter/pyspark-notebook:python-3.8.8

COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN rm -f requirements.txt