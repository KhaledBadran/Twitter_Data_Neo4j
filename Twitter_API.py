import json

import tweepy as tw
import re
import pprint
import csv
import DB_Utility
import Constants
from textblob import TextBlob
import Neo4J_DB


def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\ / \ / \S+)", " ", tweet).split())


def get_tweet_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    # create TextBlob object of passed tweet text
    analysis = TextBlob(clean_tweet(tweet))
    # set sentiment
    if analysis.sentiment.polarity > 0.25:
        return 'positive'
    elif analysis.sentiment.polarity <= 0.075:
        return 'negative'
    else:
        return 'neutral'



def create_user_using_user_screen_name(api, user_screen_name):
    user_account = api.get_user(screen_name=user_screen_name)   # call twitter api to get the leader twitter data
    ## User Data
    user = {}
    user["id"] = user_account.id
    user["screen_name"] = user_account.screen_name
    user["followers"] = user_account.followers_count
    user["favourites_count"] = user_account.favourites_count
    user["following_count"] = user_account.friends_count
    user["verified"] = user_account.verified
    user["location"] = user_account.location
    return user

def create_user_using_user_id(api, user_id):
    user_account = api.get_user(id=user_id)   # call twitter api to get the leader twitter data
    ## User Data
    user = {}
    user["id"] = user_account.id
    user["screen_name"] = user_account.screen_name
    user["followers"] = user_account.followers_count
    user["favourites_count"] = user_account.favourites_count
    user["following_count"] = user_account.friends_count
    user["verified"] = user_account.verified
    user["location"] = user_account.location
    return user

def find_user_city(user_location):

    for city in Constants.Cities_Countries:
        if city.lower() in user_location.lower():
            return city

    return "unknown"

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tw.API(auth)

# Define the search term and the date_since date as variables
search_words = "coronavirus"
date_since = "2020-03-027"
new_search = search_words + " -filter:retweets"

users = {}
users_tweets = {}

max_tweets = 250

db_util = Neo4J_DB.Neo4j_DB_Util()

for country, leader_screen_name in Constants.Countries_Leaders.items():
    leader_account = create_user_using_user_screen_name(api, leader_screen_name) # call twitter api to get the leader twitter data
    print(leader_account)

    db_util.Insert_User(leader_account)  # Insert User (leader) into DB
    db_util.Connect_Leader_Country(leader_account['id'], country)  # Connect leader user account to country



for key in Constants.Cities_Geocode:

      # Collect tweets
      tweets = tw.Cursor(api.search,
                         tweet_mode='extended',
                         wait_on_rate_limit=True,
                         wait_on_rate_limit_notify=True,
                         q=new_search,
                         lang="en",
                         since=date_since,
                         count=max_tweets,
                         geocode=Constants.Cities_Geocode[key]).items(max_tweets)

      # Iterate and print tweets
      for tweet in tweets:


          ## Tweet Data
          tweet_data = {}
          tweet_data['id'] = tweet.id
          tweet_data["text"] = tweet.full_text
          tweet_data["creation_time"] = tweet.created_at
          tweet_data["retweet_count"] = tweet.retweet_count
          tweet_data["favorite_count"] = tweet.favorite_count
          tweet_json = json.loads(json.dumps(tweet._json))
          users_mentioned = tweet_json['entities']['user_mentions']
          #tweet_data["user_mentions"] = [user['id'] for user in users_mentioned]
          tweet_hashtags = tweet_json['entities']['hashtags']
          tweet_data["hashtags"] = [hashtag['text'] for hashtag in tweet_hashtags]
          tweet_data["sentiment"] = get_tweet_sentiment(tweet.full_text)
          tweet_data["tweeted_by"] = tweet.user.id

          print("--------------------------------------------- Tweet Data ---------------------------------------------\n")
          pprint.pprint(tweet_data)

          db_util.Insert_Tweet(tweet_data)    #Insert Tweet into DB



          ## User Data
          user = {}
          user["id"] = tweet.user.id
          user["screen_name"] = tweet.user.screen_name
          user["followers"] = tweet.user.followers_count
          user["favourites_count"] = tweet.user.favourites_count
          user["following_count"] = tweet.user.friends_count
          user["verified"] = tweet.user.verified
          user["location"] = tweet.user.location
          #print("--------------------------------------------- User Data ---------------------------------------------\n")
          #pprint.pprint(user)
          print(tweet.user.location)

          db_util.Insert_User(user)    #Insert User into DB
          db_util.Connect_User_City(tweet.user.id, key)    # Connect user with the city
          db_util.Insert_Hashtag(tweet_data["hashtags"], tweet.user.id, tweet.id)    #Insert list of hasgtags into DB & Connect them to user and tweet

          for user in users_mentioned:
              db_util.Insert_Mentioned_User(user['id'], user['screen_name'])     # Create a mentioned user entity
              db_util.Connect_Mentioned_User_Tweet(user['id'], tweet.id)      # Connect user with the city

          ## Mentioned Users:


          # print([tweet.id, tweet.created_at, key, sentiment, tweet.user.id, t_text])
          # pprint.pprint(json.loads(tweet))

          # convert to string
          # json_str = json.dumps(tweet._json)

          # deserialise string into python object
          # parsed = json.loads(json_str)

          # print(json.dumps(parsed, indent=4, sort_keys=True) + ",")
          # DB_Utility.dbInsert(tweet.id, t_text.encode('utf-8'), sentiment)



db_util.Connect_User_Tweet()
db_util.close()
