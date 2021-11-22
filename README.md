# <b>Spark tweet analysis<br></b>

This is Our Project for the Cloud Computing Course. This project will take user input of keywords and pages and fetch
the tweets based on those keywords from twitter, filter hashtags from those tweets and give those hashtags to spark for
processing.

### Objective of the project:

To create a program using Apache Spark, an online streaming Real-Time Analytics Platform, to process the tweets and
identifying the trending hashtags from Twitter based on a certain keyword and, finally, retrieve top hashtags by
representing the data in a real-time dashboard.

### Limitations

- 450 queries per 15 minutes (enforced by twitter APIv2)
- 500K queries per month(enforced by twitter APIv2)
- We cannot get general tweets from Twitter. We have to get tweets based on some Query string (enforced by twitter
  APIv2)

### Dataset used in this project

The dataset used for this project is Twitter tweets. In, order to get the Twitter tweets we need Twitter API, for that
first, we need to register for the Twitter Developer account. After the registration click on the 'create an
application' and, finally, generate the Bearer Token for the project.<br>

Make a new file ```keys.txt``` and in it put the bearer token in below format
```
token:<your_token_here>
```

make sure there are no spaces between token & : and : & <your_token>  

### Methodology used in developing project:

<img src="/asset/methodology.png"/>

- First, the tweets from Twitter has received to the machine using the Twitter APIv2.
- The tweets are based on keywords that user specifies. (see running the app section)
- A python program has implemented to receive the data and then sent through a TCP Socket to spark.
- The data been processed with the pyspark using Apache spark and then the Twitter tweets and trending hashtag would be
  shown.
- To display the data in a visual representation, we are using flask web app.

### Running the Application

First steps first...

- Java version should be compatible with pyspark. Current version of pyspark is 3.2.0 and only java
version 11 is compatible. You can check java version
by running command ```java --version```
- ```git clone https://github.com/HritwikSinghal/Spark-tweet.git```
- ```cd Spark-tweet```
- ```pip install -r ./requirements.txt```

#### 1. Automatic run

Simply run ```run.sh```. if you want the defaults. The defaults are :

- keywords = "corona bitcoin gaming Android"
- pages = 2 (per keyword)

Note that this will open the browser window and will kill the app after 2 minutes.
(this will not happen if you use manual run, although you can modify ```run.sh``` to change this behaviour)

#### 2. Manual run

Run the Programs in the order. **NOTE: Every step should be run in new terminal** <br>

1. Flask Application ```python3 ./app.py```


2.

```
  export PYSPARK_PYTHON=python3
  export SPARK_LOCAL_HOSTNAME=localhost
  python3 ./spark_app.py
  ```

3. ```python3 ./twitter_app.py -p _<no_of_pages>_ -k _<"keywords">_```

Replace ```_<"keywords">_``` with the keywords you want to search
(Note that keywords should be in quotes, like ```"corona bitcoin gaming Android"```)

and ```<no_of_pages>``` with the number of pages you want for each keyword from twitter.

### Visual representation

You can access the real-time data in visual representation by accessing this URL given below.

```
http://localhost:5001/ 
```

or

```
http://127.0.0.1/5001
```

### Stopping the application

run ```killall python3```  in new terminal

### Final Output

TBD

<img src="/asset/footer.png"/>