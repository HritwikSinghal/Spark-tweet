import time
from socket import *
import socket
import traceback

import requests

import json


def auth():
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
    params['next_token'] = next_token
    response = requests.get(url, headers=headers, params=params)
    print(f"Endpoint Response Code: {str(response.status_code)}")
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def get_tweet_data(next_token=None):
    # Inputs for the request
    bearer_token = auth()
    headers = {"Authorization": f"Bearer {bearer_token}"}

    keyword = "corona lang:en has:hashtags"
    end_time = "2021-11-15T00:00:00.000Z"

    url: tuple = create_url(keyword, end_time, max_results=20)
    json_response = get_response(url=url[0], headers=headers, params=url[1], next_token=next_token)

    # print(json.dumps(json_response, indent=4))
    with open('test.txt', 'w+') as teeee:
        json.dump(json_response, teeee, indent=2)

    return json_response


def send_tweets_to_spark(http_resp, tcp_connection):
    data: list = http_resp["data"]

    # tweet is a dict
    for tweet in data:
        try:
            # print(tweet)
            hashtag_arr = tweet['entities']['hashtags']
            for _ in hashtag_arr:
                tag = _['tag']
                tweet_text = str('#' + tag + '\n').encode("utf-8")  # pyspark can't accept stream, add '\n'
                print(f"Hashtag: {tweet_text}\n------------------------------------------")
                tcp_connection.send(tweet_text)
        except Exception as e:
            traceback.print_exc()


if __name__ == '__main__':
    TCP_IP = "127.0.0.1"
    TCP_PORT = 9009

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)

    print("Waiting for the TCP connection...")

    conn, addr = s.accept()
    print("Connected successfully... Starting getting tweets.")

    next_token = None
    for _ in range(2):
        print(f"\n\n\n\n\nProcessing Page{_}\n\n\nn\\n")
        resp = get_tweet_data(next_token=next_token)
        next_token = resp['meta']['next_token']
        send_tweets_to_spark(resp, conn)
        time.sleep(3)

# ------------------------------------------------------------------ #

# get_tweet_data()
# start()
