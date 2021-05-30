import tweepy
# import nltk
import pandas as pd
import os
import time
import pystore
import threading
from nltk.sentiment.vader import SentimentIntensityAnalyzer
# nltk.download('vader_lexicon')


class SentimentIterator:
    def __init__(self, sentiment):
        self._sentiment = sentiment
        self._index = 0

    def __next__(self):
        return self.sentiment.get_next()


class Sentiments:
    def __init__(self, consumerKey, consumerSecret, keyword, noOfTweets, lastTweetId=0):
        self.keyword = keyword
        self.noOfTweets = noOfTweets
        self.lastTweetId = lastTweetId
        auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
        self.twitter = tweepy.API(auth)
        self.tweets = pd.DataFrame()

    def __iter__(self):
        return SentimentIterator(self)

    def get_next(self):
        tweets = self.get_tweets(self.lastTweetId)
        result = pd.DataFrame()
        if not tweets.empty:
            self.tweets = pd.concat([self.tweets, tweets])
            self.lastTweetId = self.tweets['tweet_id'].iloc[-1]
            lastTS = tweets.index[-1]
            lastMinute = pd.Timestamp(
                lastTS.year, lastTS.month, lastTS.day, lastTS.hour, lastTS.minute)
            result = self.group_tweets(
                self.tweets.loc[:lastMinute - pd.Timedelta(seconds=1)])
            self.tweets = self.tweets[lastMinute:]
        return result

    def group_tweets(self, tweets):
        id_first = tweets.groupby(pd.Grouper(freq='Min')).first()
        id_last = tweets.groupby(pd.Grouper(freq='Min')).last()
        result = tweets.drop(columns='tweet_id').groupby(
            pd.Grouper(freq='Min')).sum()
        result['first_id'] = id_first['tweet_id']
        result['last_id'] = id_last['tweet_id']
        return result

    def get_tweets(self, sinceId=0):
        def limit_handle(cursor):
            while True:
                try:
                    yield cursor.next()
                except StopIteration:
                    return
                except tweepy.RateLimitError:
                    print('Twitter API Rate Limit!')
                    return
                except tweepy.TweepError as err:
                    print(err)
                    return
        cursor = tweepy.Cursor(
            self.twitter.search,
            q=self.keyword,
            count=500,
            since_id=sinceId,
            include_entities=False
        ).items(self.noOfTweets)
        tweet_list = []
        for tweet in limit_handle(cursor):
            score = SentimentIntensityAnalyzer().polarity_scores(tweet.text)
            # analysis = TextBlob(tweet.text)
            tweet_list.append({
                'timestamp': tweet.created_at,
                'positive': 1 if score['pos'] > score['neg'] else 0,
                'negative': 1 if score['pos'] < score['neg'] else 0,
                'neutral': 1 if score['pos'] == score['neg'] else 0,
                'tweet_id': tweet.id})
        tweets = pd.DataFrame(tweet_list, columns=[
                              'timestamp', 'positive', 'negative', 'neutral', 'tweet_id']).set_index('timestamp')
        return tweets.iloc[::-1]


def task(sentiments, collection, keyword, sleepTime):
    while True:
        print(f'scanning {keyword} ...')
        data = sentiments.get_next()
        if not data.empty:
            if keyword in collection.list_items():
                collection.append(keyword, data)
            else:
                collection.write(keyword, data)
        time.sleep(sleepTime)

def start(keywords, consumerKey, consumerSecret, storeName='tsdb', collectionName='twitter', tweetCount='500', sleepTime='30'):
    store = pystore.store(storeName)
    collection = store.collection(collectionName)
    print(f'starting ... {keywords}')
    threads = []
    for keyword in keywords:
        lastTweetId = 0
        if keyword in collection.list_items():
            item = collection.item(keyword)
            lastTweetId = item.to_pandas()[:-1]['last_id']
        sentiments = Sentiments(
            consumerKey, consumerSecret, keyword, tweetCount, lastTweetId)
        
        threads.append(threading.Thread(target=task, args=(sentiments, collection, keyword, sleepTime)))
    
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def main():
    KEYWORDS = os.getenv("KEYWORDS", "bitcoin,etherium,crypto").split(',')
    CONSUMER_KEY = os.getenv("CONSUMER_KEY")
    CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
    STORE_NAME = os.getenv("STORE_NAME", "tsdb")
    COLLECTION_NAME = os.getenv("COLLECTION_NAME", "twitter")
    SLEEP_TIME = int(os.getenv("SLEEP_TIME", 30))
    TWEET_COUNT = int(os.getenv("TWEET_COUNT", 500))

    start(KEYWORDS, CONSUMER_KEY,
                CONSUMER_SECRET, STORE_NAME, COLLECTION_NAME, TWEET_COUNT, SLEEP_TIME)



if __name__ == "__main__":
    main()
