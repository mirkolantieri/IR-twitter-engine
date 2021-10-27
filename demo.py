#!/usr/bin/env python3

from utils.utils import *
from indexer import indexDocuments
from preprocessor import Preprocessor

from elasticsearch import Elasticsearch


def basicQueries():
    '''
    Performs standard queries on the news twitter index
    '''

    def search(index, query=None, n_res=10):
        es = Elasticsearch()
        res = es.search(index=index_name, body={
            "query" : query
        }, size=n_res)

        pprint(g("%d documents found (showing first %d)" % (res['hits']['total']['value'], n_res)))
        printRes(res)
        return res


    pprint(r("BASIC QUERIES DEMO"))

    ## USER CASE 1 - Textual search on a specific field using keywords ##
    
    # Query 1 - about politics topic
    search(index_name, query={
                "match" : {
                    "text" : "USA elections fraud claims"
                }})

    # Query 2 - about science topic
    search(index_name, query={
                "match" : {
                    "text" : "NASA started new mission on mars"
                }})    

    # Query 3 - about other topics
    search(index_name, query={
                "match_phrase" : {
                    "text" : "latest products relased by Apple in 2020"
                }})    


    ## USER CASE 2 - Textual search on a combination of fields ##

    # Query 4 - tweets by sport news accounts that talk about NBA 
    search(index_name, query={
                "bool": {
                    "must": [
                        { "bool": {
                            "should" : [
                                { "match": {"screen_name":"YahooSports"} },
                                { "match": {"screen_name":"FOXSports"} },
                                { "match": {"screen_name":"cnnsport"} }
                            ],
                            "minimum_should_match": 1
                        }},
                        {"match": {"text":"LeBron James pays tribute to Kobe Bryant"}}
                    ]
                }})


    # Query 5 - range query on midnight on New Year's Eve about wishes
    search(index_name, query={
                "bool" : {
                    "must" : [
                        { "range": {
                            "date" : {
                                "gt" : "Sat Dec 12 00:00:00 +0000 2020",
                                "lte" : "Mon Dec 14 23:00:00 +0000 2020"                    
                            }
                        }},
                        {"match": {"text":"Covid19 Pfizer vaccine approvals"}}
                    ]
                }})
    


def advancedQueries(users_tweets):
    '''
    Performs advanced queries on the news twitter index, customizing the results based on the tweets of the users 
    considered.
    '''

    def search(index, query=None, n_res=100):
        es = Elasticsearch()
        res = es.search(index=index_name, body={
            "query" : query
        }, size=n_res)

        return res


    pprint(r("ADVANCED QUERIES DEMO"))

    ## USER CASE 3 - Rank the tweets taking into account the user profile ##

    # If user list is empty personalize_query retrive the search personalized for each user in dataset, else it provides
    # personalization for only the users specified.
    # Avaiable Twitter users in dataset:
    #   - Group one (scientists)
            # Neil deGrasse Tyson
            # Katie Mack
            # Brian Cox
    #   - Group two (politicians)
            # Barack Obama
            # Nancy Pelosi
            # Joe Biden

    user = ['Joe Biden','Brian Cox']
    # ES standard query
    query_res = search(index_name, query={
                    "bool" : {
                        "must" : [
                            {"match" : {"text" : "What this pandemic year can teach us about"}},
                            {"match" : {"text" : "coronavirus"}}
                        ]
                    }})
    # Personalization re-rank process
    personalized_res = users_tweets.personalize_query(query_res, user)
    #printRes(query_res)
    printResAdv(personalized_res)


    ## USER CASE 4 - Expand the search adding synonyms of the words in the query ##

    # Query  - expanding previous query with synonyms
    query_res = search(index_name, query={
                    "bool" : {
                        "must" : [
                            {"match" : {"text" : {
                                "query" : "What this pandemic year can teach us about",
                                "analyzer" : "synonym"}}},
                            {"match" : {"text" : "coronavirus"}}
                        ]
                    }})
    
    # Personalization re-rank process
    personalized_res = users_tweets.personalize_query(query_res, user)
    #printRes(query_res)
    printResAdv(personalized_res)



## Utils functions
def printRes(res):
    for doc in res['hits']['hits']:
        print(y("Tweet ID: ") + doc['_id'] + 
                g("\nUser: ") + doc['_source']['user_name'] +
                g("\nCreated at: ") + doc['_source']['date'] +
                g("\nText: ") + doc['_source']['text'] + 
                r("\nScore: ") + str(doc['_score']) + "\n")

def printResAdv(res):
    for usr in res:
        pprint('Personalized results for user: ' + usr)
        for doc in res[usr]['news']:
            print(y("Tweet ID: ") + doc['_id'] + 
                    g("\nUser: ") + doc['_source']['user_name'] +
                    g("\nCreated at: ") + doc['_source']['date'] +
                    g("\nText: ") + doc['_source']['text'] + 
                    r("\nPersonalized score: ") + str(doc['new_score']) + "\n")



if __name__ == "__main__":

    ## ES parameters for indexing data
    index_name = 'twitter_index'
    data_path = './datasets/news_tweets.json'
    config_path = './utils/index_config.json'

    ## Index document specified in ES server, only the first time
    #indexDocuments(data_path, config_path, index_name)

    ## Basic queries on Elasticsearch
    basicQueries()

    ## User tweets-based personalization
    users_tweets_path = ["./datasets/group_one.json","./datasets/group_two.json"]
    users_tweets = Preprocessor(users_tweets_path)

    ## Avanced queries using users tweets for personalization
    advancedQueries(users_tweets)
