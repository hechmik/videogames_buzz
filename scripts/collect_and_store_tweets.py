from pymongo import MongoClient
import tweepy
import json

def load_twitter_credentials(keys_filename):
    consumer_key, consumer_secret, access_token, access_secret = "", "", "", ""
    with open(keys_filename) as api_credentials:
        api_twitter = json.loads(api_credentials.read())
        consumer_key = api_twitter['Twitter']["consumer_key"]
        consumer_secret = api_twitter['Twitter']["consumer_secret"]
        access_token = api_twitter['Twitter']["access_token"]
        access_secret = api_twitter['Twitter']["access_secret"]
        return(consumer_key, consumer_secret, access_token, access_secret)

def twitter_api_setup(consumer_key, consumer_secret, access_token, access_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    #Create API
    api = tweepy.API(auth)
    return(api)
def collect_and_store_tweets_from_query(api, query, since_date, until_date, mongo_collection):
    tweets = tweepy.Cursor(api.search,
              q = query,
              since = since_date,
              until = until_date).items()
    for tweet in tweets:
        filtered_tweet = {
            "query"    : query,
            "text"     : tweet.text,
            "language" : tweet.lang,
            "date"     : tweet.created_at,
            "username" : tweet.user.name,
            "user_followers": tweet.user.followers_count,
            "user_location": tweet.user.location,
            "retweets" : tweet.retweet_count,
            "likes"    : tweet.favorite_count,
        }
        mongo_collection.insert_one(filtered_tweet)

def collect_and_store_tweets_from_multiple_queries(keys_filename,list_of_queries, since_date, until_date, ip_mongo, mongo_port):
    consumer_key, consumer_secret, access_token, access_secret = load_twitter_credentials(keys_filename)
    api = twitter_api_setup(consumer_key, consumer_secret, access_token, access_secret)
    client = MongoClient(ip_mongo, mongo_port)
    db = client.twitter
    tweet_games = db.game_tweets
    for query in list_of_queries:
        print("Working on: {}".format(query))
        collect_and_store_tweets_from_query(api, query, since_date, until_date, tweet_games)
