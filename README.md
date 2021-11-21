
# <b>Spark tweet analysis<br></b>

This is Our Project for the course Cloud Computing

### Objective of the project:

To create a program using Apache Spark an online streaming Real-Time Analytics Platform to process the tweets and
identifying the trending hashtags from Twitter based on a certain keyword and, finally, retrieve top hashtags and representing the data in a
real-time dashboard.

### Limitations
- 450 queries per 15 minutes (enforced by twitter)
- 500K queries per month(enforced by twitter)
- We cannot get general tweets from Twitter. We have to get tweets based on some Query string (enforced by twitter) 

### Datasetused in this porject

The dataset used for this project is Twitter tweets. In, order to get the Twitter tweets we need Twitter API, for that
first, we need to register for the Twitter Developer account. After the registration click on the create an application
and, finally, generate the access token for the project.<br>

### Methodology used in developing project:

<img src="/asset/methodology.png"/>

- First, the tweets from Twitter has received to the machine using the Twitter APIv2.
- A python program has implemented to receive the data and then sent through a TCP Socket.
- The data been processed with the pyspark using Apache spark and then the Twitter tweets and trending hashtag would be
  outputted.
- To display the data using flask micro web app to display the data in a visual representation.

### Running the Application

Simply run ```run.sh```.

### Visual representation

You can access the real-time data in visual representation by accessing this URL given below.

```
<http://localhost:5001/ > or <http://127.0.0.1/5001>
```

### Stopping the application

run ```killall python3```


### Final Output

TBD

<img src="/asset/footer.png"/>