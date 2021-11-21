
<h1 align="center">
  <b>Twitter Tweet Analysis<br></b>
</h1>
<p align="center">
  <img src="https://img.shields.io/badge/Library-Flask-red.svg?style=flat-square">
  <img src="https://img.shields.io/badge/Tool-ApacheSpark-yellow.svg?style=flat-square">
  <img src="https://img.shields.io/badge/Language-Python-purple.svg?style=flat-square">
</p>

## Spark tweet analysis
This is Our college assignment for the course CC

### Objective of the project:
To create a program using Apache Spark an online streaming Real-Time Analytics Platform to process the tweets and identifying the trending hashtags from Twitter and, finally, retrieve top hashtags and representing the data in a real-time dashboard.

### Datasetused in this porject
The dataset used for this project is Twitter tweets. In, order to get the Twitter tweets we need Twitter API, for that first, we need to register for the Twitter Developer account. After the registration click on the create an application and, finally, generate the access token for the project.<br>


### Methodology used in developing project:
<img src="/asset/methodology.png"/>

- First, the tweets from Twitter has received to the machine using the Twitter API.
- A python program has implemented to receive the data and then sent through a TCP Socket.
- The data been processed with the pyspark using Apache spark and then the Twitter tweets and trending hashtag would be outputted.
- To display the data using flask micro web app to display the data in a visual representation.

### Running the Application
Simply run ```run.sh```.

### Visual representation
You can access the real-time data in visual representation by accessing this URL given below.

### Stopping the application
run ```killall python3```

```
<http://localhost:5001/ > or <http://127.0.0.1/5001>
```

### Final Output
TBD

<img src="/asset/footer.png"/>