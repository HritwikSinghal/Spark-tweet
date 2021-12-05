import os

import pyspark
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"


def send_data(tags: dict) -> None:
    url = 'http://localhost:5001/updateData'

    # request_data = {'label': str(top_tags), 'data': str(tags_count)}
    # response = requests.post(url, data=request_data)

    response = requests.post(url, json=tags)


def process_row(row: pyspark.sql.types.Row) -> None:
    # print(type(row))  # pyspark.sql.types.Row
    # print(row)            # Row(hashtag='#BSCGems', count=2)
    tags = row.asDict()
    print(tags)  # {'hashtag': '#Colorado', 'count': 1}
    send_data(tags)


def new():
    # create a local SparkSession, the starting point of all functionalities related to Spark
    spark = SparkSession.builder.appName("SparkTwitterAnalysis").getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # This 'lines' DataFrame represents an unbounded table containing the streaming text data.
    # This table contains one column of strings named “value”,
    # and each line in the streaming text data becomes a row in the table

    # create a streaming DataFrame that represents text data received from a server listening on localhost:9009
    lines = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 9009).load()
    # print(type(lines))        # class 'pyspark.sql.dataframe.DataFrame'

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
    query = wordCounts.writeStream.foreach(process_row).outputMode('Update').start()
    # print(type(query))          # class 'pyspark.sql.streaming.StreamingQuery'

    query.awaitTermination()


if __name__ == '__main__':
    try:
        new()
    except BrokenPipeError:
        exit("Pipe Broken, Exiting...")
    except KeyboardInterrupt:
        exit("Keyboard Interrupt, Exiting..")
    except Exception as e:
        # traceback.print_exc()
        exit("Error in Spark App")
