import socket
import traceback

import requests

import json


def auth():
    # return os.getenv('TOKEN')
    with open('keys.txt', 'r') as key_file:
        lines = key_file.read().split('\n')
        for x in lines:
            x = x.strip()
            if x.startswith("token:"):
                return x[6:]


def create_url(keyword, end_date, max_results=10):
    search_url = "https://api.twitter.com/2/tweets/search/recent"

    # change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'end_time': end_date,
                    'max_results': max_results,
                    'tweet.fields': 'id,text,author_id,geo,conversation_id,created_at,lang,entities',
                    'next_token': {}
                    }
    return (search_url, query_params)


def get_response(url, headers, params, next_token=None):
    params['next_token'] = next_token  # params object received from create_url function
    response = requests.get(url, headers=headers, params=params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def send_tweets_to_spark(http_resp, tcp_connection):
    data: list = http_resp["data"]

    # tweet is a dict
    for tweet in data:
        try:
            tweet_text = tweet['text'].encode("utf-8") + '\n'  # pyspark can't accept stream, add '\n'
            print(f"Tweet Text: {tweet_text}\n------------------------------------------")
            tcp_connection.send(tweet_text + '\n')
        except Exception as e:
            traceback.print_exc()


def get_tweet_data():
    # Inputs for the request
    bearer_token = auth()
    headers = {"Authorization": f"Bearer {bearer_token}"}

    keyword = "xbox lang:en has:hashtags"
    end_time = "2021-11-15T00:00:00.000Z"

    url = create_url(keyword, end_time)
    json_response = get_response(url[0], headers, url[1])

    # print(json.dumps(json_response, indent=4))
    with open('test.txt', 'w+') as teeee:
        json.dump(json_response, teeee, indent=2)

    return json_response


def start():
    TCP_IP = "127.0.0.1"
    TCP_PORT = 9009
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("Waiting for the TCP connection...")
    conn, addr = s.accept()
    print("Connected successfully... Starting getting tweets.")
    resp = get_tweet_data()
    send_tweets_to_spark(resp, conn)


# ------------------------------------------------------------------ #

# get_tweet_data()
start()
