#!/usr/bin/env python3

import argparse
import json, csv
import pandas as pd
import sys
from datetime import datetime, timedelta
from math import ceil
from os import path, makedirs

import tweepy
from api_key import key
from requests import get, codes
from requests_oauthlib import OAuth1


# CONSTANTS
DATE_FORMAT = "%Y-%m-%d"
TWEET_LIMIT = 3200  # recent tweets
API_LIMIT = 200  # tweets at once
OUT_DIR = 'out/' # folder of outputs

# OUTPUT COLORS
RESET = "\033[0m"
bw = lambda s: "\033[1m\033[37m" + str(s) + RESET  # bold white
w = lambda s: "\033[1m" + str(s) + RESET  # white
g = lambda s: "\033[32m" + str(s) + RESET  # green
y = lambda s: "\033[33m" + str(s) + RESET  # yellow


class Scraper:

    def __init__(self, handle, multiUsr=False):
        self.api = self.__authorize()
        self.handle = handle.lower()
        if multiUsr:
            self.outfile = "all_usrs.json"
        else:
            self.outfile = self.handle + ".json"
        
        self.new_tweets = set()  # ids
        self.tweets = self.__retrieve_existing()  # actual tweets

    @staticmethod
    def __authorize():
        consumer_key = key["consumer_key"]
        consumer_secret = key["consumer_secret"]
        access_token = key["access_token"]
        access_token_secret = key["access_token_secret"]

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)
        return api

    def __retrieve_existing(self):
        tweets = dict()
        if path.exists(OUT_DIR + self.outfile):
            with open(OUT_DIR + self.outfile) as o:
                tweets = json.load(o)

        return tweets

    def __check_if_scrapable(self):
        try:
            u = self.api.get_user(self.handle)
            if not u.following and u.protected:
                exit("Cannot scrape a private user unless this API account is following them.")
        except tweepy.TweepError as e:
            if e.api_code == 50:
                exit("This user does not exist.")
            else:
                raise e

    def scrape(self, start, end, retweet):
        self.__check_if_scrapable()
        pprint(g("scraping user"), w("@") + y(self.handle) + g("..."))
        pprint(g("including retweet: ") + w(retweet))
        pprint(w(len(self.tweets)), g("existing tweets in"), y(self.outfile))
        self.__quickscrape(start, end, retweet)
        


    def __quickscrape(self, start, end, retweet):
        # can't use Tweepy, need to call actual API
        def authorize():
            return OAuth1(key["consumer_key"], key["consumer_secret"], key["access_token"], key["access_token_secret"])

        def form_initial_query():
            base_url = "https://api.twitter.com/1.1/statuses/user_timeline.json"
            query = "?screen_name={}&count={}&tweet_mode=extended".format(self.handle, API_LIMIT)
            return base_url + query

        def form_subsequent_query(max_id):
            base_url = "https://api.twitter.com/1.1/statuses/user_timeline.json"  # don't use the is_retweet field!
            query = "?screen_name={}&count={}&tweet_mode=extended&max_id={}".format(self.handle, API_LIMIT, max_id)
            return base_url + query

        def make_request(query):
            request = get(query, auth=authorize())
            if request.status_code == codes.ok:
                return dict((tw["id_str"], tw) for tw in request.json())
            else:
                request.raise_for_status()

        def retrieve_payload():
            recent_payload = make_request(form_initial_query())  # query initial 200 tweets
            all_tweets = dict(recent_payload)

            for _ in range(ceil(TWEET_LIMIT / API_LIMIT) - 1):  # retrieve the other 3000 tweets
                oldest_tweet = list(recent_payload.keys())[-1]  # most recently added tweet is oldest
                recent_payload = make_request(form_subsequent_query(max_id=oldest_tweet))
                all_tweets.update(recent_payload)
            return all_tweets

        def filter_tweets(tweets, retweet):
            def get_date(tw):  # parse the timestamp as a datetime and remove timezone
                return datetime.strptime(tw[1]["created_at"], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)

            def is_retweet(tw): # check if is a retweet
                return "retweeted_status" in tw[1]

            if retweet: # want also the retweets
                return dict(filter(lambda tweet: start <= get_date(tweet) <= end, tweets.items()))
            else:       # don't want the retweets
                return dict(filter(lambda tweet: start <= get_date(tweet) <= end and not is_retweet(tweet), tweets.items()))

        def extract_metadata(tweets):
            tweets_dict = {}

            for id, tw in tweets.items():
                hashtags = []
                try:
                    for hashtag in tw["entities"]["hashtags"]:
                        hashtags.append(hashtag["text"])
                except:
                    pass
                tweets_dict.update({ id : {
                    'user_name': tw["user"]["name"],
                    'screen_name': tw["user"]["screen_name"],
                    'date': tw["created_at"],
                    'tweet_id' : id,
                    'text': tw["full_text"],
                    'hashtags': hashtags if hashtags else [None]}
                })  
            return tweets_dict

        new_tweets = extract_metadata(filter_tweets(retrieve_payload(), retweet))
        pprint(g("found"), w(len(new_tweets.keys() - self.tweets.keys())), g("new tweets"))
        self.tweets.update(new_tweets)

    def dump_tweets(self):
        # write out to json
        if not path.exists(OUT_DIR):
            makedirs(path.dirname(OUT_DIR), exist_ok=True)
        with open(OUT_DIR + self.outfile, "w") as o:
            json.dump(self.tweets, o, indent=4)
        pprint(g("stored tweets in"), y(OUT_DIR + self.outfile))


# HELPER functions
def get_join_date(handle):
    """
    Helper method - checks a user's twitter page for the date they joined
    :return: the "%day %month %year" a user joined
    """
    baby_scraper = Scraper(handle)
    join_date = baby_scraper.api.get_user(handle).created_at
    return join_date

def pprint(*arguments):  # output formatting
    print(bw("["), *arguments, bw("]"))



if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="scrape.py", usage="python3 %(prog)s [options]",
                                     description="scrape.py - Twitter Scraping Tool")
    parser.add_argument("-u", "--username", help="Scrape this user's Tweets.")
    parser.add_argument("-f", "--file", help="Scrape user's Tweets in txt file.")
    parser.add_argument("--since", help="Get Tweets after this date (Example: 2010-01-01).")
    parser.add_argument("--until", help="Get Tweets before this date (Example: 2018-12-07).")
    parser.add_argument("--retweet", help="Get Tweets retweetted (Example: True or False). The default is False.")
    args = parser.parse_args()

    if args.retweet is not None:
        if args.retweet=='True' or args.retweet=='False':
            retweet = True if args.retweet=='True' else False
        else:
            print("ERROR: Invalid value for --retweet.")
            sys.exit()
    else: # retweet False at default
        retweet = False

    if args.username is None:
        # multi-user search
        if args.file is not None:
            try:
                users = pd.read_csv(args.file, header=None)
            except:
                exit("This file does not exist or is empty.")
            
            for usr in users[0]:
                if not usr.startswith("#"):
                    begin = datetime.strptime(args.since, DATE_FORMAT) if args.since else get_join_date(usr)
                    end = datetime.strptime(args.until, DATE_FORMAT) if args.until else datetime.now()

                    user = Scraper(usr, multiUsr=True)
                    user.scrape(begin, end, retweet)
                    user.dump_tweets()
        else:   
            print("ERROR: No username or usernames file given, terminating.")
            sys.exit()
    # single user search
    else: 
        begin = datetime.strptime(args.since, DATE_FORMAT) if args.since else get_join_date(args.username)
        end = datetime.strptime(args.until, DATE_FORMAT) if args.until else datetime.now()

        user = Scraper(args.username)
        user.scrape(begin, end, retweet)
        user.dump_tweets()