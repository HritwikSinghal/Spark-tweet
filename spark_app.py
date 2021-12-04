import os
import traceback

import requests
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext
from pyspark.streaming import StreamingContext

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def get_sql_context_instance(spark_context):
    if 'sqlContextSingletonInstance' not in globals():
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def send_df_to_dashboard(df):
    top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
    tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)


def process_rdd(time, rdd):
    print(f"----------- {str(time)} -----------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0].encode("utf-8"), hashtag_count=w[1]))
        hashtags_df = sql_context.createDataFrame(row_rdd)
        hashtags_df.registerTempTable("hashtags")
        hashtag_counts_df = sql_context.sql(
            "select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 25")
        hashtag_counts_df.show()
        send_df_to_dashboard(hashtag_counts_df)
    except Exception as e:
        traceback.print_exc()


if __name__ == '__main__':
    conf = SparkConf()
    conf.setAppName("SparkTwitterAnalysis")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")

    ssc = StreamingContext(sc, 2)
    ssc.checkpoint("checkpoint_TwitterApp")

    dataStream = ssc.socketTextStream("127.0.0.1", 9009)

    # First we’ll split all the tweets into words and put them in words RDD
    words = dataStream.flatMap(lambda line: line.split(" "))

    # Then we’ll filter only hashtags from all words
    hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))
    # and map them to pair of (hashtag, 1)
    tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
    # and put them in hashtags RDD
    tags_totals.foreachRDD(process_rdd)
    ssc.start()
    ssc.awaitTermination()
