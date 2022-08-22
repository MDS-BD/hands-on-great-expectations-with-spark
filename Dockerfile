FROM jupyter/pyspark-notebook:619e9cc2fc07

ENV PYTHONPATH="/usr/local/spark-2.4.5-bin-hadoop2.7/python/lib/pyspark.zip;/usr/local/spark-2.4.5-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip"

COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN rm -f requirements.txt