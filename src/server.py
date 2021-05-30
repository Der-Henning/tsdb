import flask
from flask import request
import os
import threading
import sentiments
import pandas as pd
import pystore

PORT = os.getenv("PORT", 80)
FLASK_ENV = os.getenv("FLASK_ENV", "production")
KEYWORDS = os.getenv("KEYWORDS", "bitcoin,etherium,crypto").split(',')
CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
STORE_NAME = os.getenv("STORE_NAME", "tsdb")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "twitter")
SLEEP_TIME = int(os.getenv("SLEEP_TIME", 30))
TWEET_COUNT = int(os.getenv("TWEET_COUNT", 500))

server = flask.Flask("tsdb")

PREFIX = "/api/v1"

def toDate(dateString): 
    return pd.Timestamp(dateString)

@server.route(PREFIX + "/<keyword>", methods=['GET'])
def get_sentiments(keyword):
    start = request.args.get('start', default = pd.Timestamp(0), type = toDate)
    end = request.args.get('end', default = pd.Timestamp.now(), type = toDate)
    store = pystore.store(STORE_NAME)
    collection = store.collection(COLLECTION_NAME)
    item = collection.item(keyword)
    result = item.to_pandas()[start:end]

    return (result.to_json(orient="index"), 200)


@server.route("/health", methods=['GET'])
def healthcheck():
    return ('', 200)


def startServer():
    from waitress import serve
    serve(server, host='0.0.0.0', port=PORT)


def startDevServer():
    server.run(host='0.0.0.0', port=PORT)


def main():
    threads = []
    threads.append(threading.Thread(target=sentiments.start, args=(KEYWORDS, CONSUMER_KEY, CONSUMER_SECRET,
                STORE_NAME, COLLECTION_NAME, TWEET_COUNT, SLEEP_TIME)))

    if FLASK_ENV == "developement":
        threads.append(threading.Thread(target=startDevServer))
    else:
        threads.append(threading.Thread(target=startServer))

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
