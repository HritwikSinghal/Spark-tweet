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

    print("\n\nDAta to be sent ------------------------------------------------------------------\n")
    print(top_tags)
    print(tags_count)
    print("\n--------------------------------------------------------------\n\n")

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


def start():
    # we import StreamingContext, which is the main entry point for all streaming functionality
    # Create a local StreamingContext with 8 working thread and batch interval of 1 second
    sc = SparkContext("local[8]", "SparkTwitterAnalysis")
    ssc = StreamingContext(sc, 3)
    sc.setLogLevel("OFF")

    # Using above context, we can create a DataStream that represents streaming data from a TCP source
    # Create a DataStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream("localhost", 9009)

    # This 'lines' DStream represents the stream of data that will be received from the data server.
    # Each record in this DStream is a line of text. Next, we want to split the lines by space into words
    # Split each line into words
    words = lines.flatMap(lambda line: line.split(" "))

    # flatMap is a one-to-many DStream operation that creates a new DStream by generating multiple new records
    # from each record in the source DStream. In this case, each line will be split into multiple words and the
    # stream of words is represented as the words DStream. Next, we want to count these words

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))

    # The words DStream is further mapped (one-to-one transformation) to a DStream of (word, 1) pairs,
    # which is then reduced to get the frequency of words in each batch of data.
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Finally, wordCounts.pprint() will print a few of the counts generated every second.
    # Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.pprint()

    # Note that when these lines are executed, Spark Streaming only sets up the computation
    # it will perform when it is started, and no real processing has started yet.
    # To start the processing after all the transformations have been setup, we finally call

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


if __name__ == '__main__':
    start()
    exit(0)

    # create a spark context
    conf = SparkConf().setAppName('SparkTwitterAnalysis')
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
