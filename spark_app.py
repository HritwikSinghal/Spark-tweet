import os
import re
import traceback

import requests
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SQLContext, SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
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


def old():
    conf = SparkConf().setAppName("SparkTwitterAnalysis")
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


tags = {}


def send_data():
    top_tags = [_ for _ in tags]
    tags_count = [tags[_] for _ in tags]
    url = 'http://localhost:5001/updateData'
    request_data = {'label': str(top_tags), 'data': str(tags_count)}
    response = requests.post(url, data=request_data)


def process_row(row, temp):
    global tags
    print(row)          # Row(hashtag='#BSCGems', count=2)
    # x = re.findall(r'hashtag=\'(.*)\', count=(.*)', str(row))[0]
    # hashtag: str = x[0]
    # count = x[1]
    # tags[hashtag] = count
    # send_data()


def new():
    # create a local SparkSession, the starting point of all functionalities related to Spark
    spark = SparkSession.builder.appName("SparkTwitterAnalysis").getOrCreate()

    # This 'lines' DataFrame represents an unbounded table containing the streaming text data.
    # This table contains one column of strings named “value”,
    # and each line in the streaming text data becomes a row in the table

    # create a streaming DataFrame that represents text data received from a server listening on localhost:9009
    lines = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 9009).load()
    # print(type(lines))        # class 'pyspark.sql.dataframe.DataFrame'
    lines.printSchema()

    # Next, we have used two built-in SQL functions - split and explode,
    # to split each line into multiple rows with a word each.
    # In addition, we use the function alias to name the new column as “word”.

    # Split the lines into words
    words = lines.select(explode(split(lines.value, " ")).alias("hashtag"))

    # Finally, we have defined the wordCounts DataFrame by grouping by the unique values in the Dataset
    # and counting them. Note that this is a streaming DataFrame which represents the running word counts of the stream.

    # Generate running word count
    wordCounts = words.groupBy("hashtag").count()
    # print(type(wordCounts))             # <class 'pyspark.sql.dataframe.DataFrame'>

    # We have now set up the query on the streaming data. All that is left is to actually start receiving data
    # and computing the counts. To do this, we set it up to print the complete set of counts
    # (specified by outputMode("complete")) to the console every time they are updated.
    # And then start the streaming computation using start().

    # Start running the query that prints the running counts to the console
    # query = wordCounts.writeStream.outputMode("complete").format("json",).start()
    # print(type(query))          # class 'pyspark.sql.streaming.StreamingQuery'

    # query = wordCounts.writeStream.foreachBatch(process_row).outputMode('Update').start()

    query = wordCounts \
        .option("checkpointLocation", "/home/hritwik/Videos") \
        .toTable("myTable")

    query.awaitTermination()


if __name__ == '__main__':
    new()
    # old()
    exit(0)
