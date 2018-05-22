from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from elasticsearch import Elasticsearch
import time

consumer_key = 'CONSUMER_KEY'
consumer_secret = 'CONSUMER_SECRET'
access_token = 'ACCESS_TOKEN'
access_token_secret ='ACCES_TOKEN_SECRET'

class StdOutListener(StreamListener):
    def on_data(self, data):
        dict_data = json.loads(data)
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(dict_data["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
        es.index(index="deneme",
                 doc_type="twitter",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": ts,
                       "message": dict_data["text"],
                      })
        print(data)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    #Connection to Twitter Streaming API
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)
    es = Elasticsearch()
    #filter keywords
    stream.filter(track=['python', 'javascript', 'ruby'])
    
