#!/bin/bash
export SPARK_HOME=/home/hritwik/Projects/test/spark-3.2.0-bin-hadoop3.2
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3
export SPARK_LOCAL_HOSTNAME=localhost
python3 ./spark_app.py &

python3 ./app.py &

python3 ./twitter_app.py &