import json
import tqdm
from os import path
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from utils.utils import *


def indexDocuments(data_path, config_path, index_name="my-index"):
    """
    Indexes a document using python library for ElasticSearch.
    Parameters
    ----------
    data_path : str
        JSON file location of documents to index.
    config_path : str
        HSON file location of index settings and mappings.
    index_name : str
        Name of index (default is 'my-index').
    """

    def genData():
        for tweet in tweets:
            yield tweets[tweet]


    # Load a JSON mapping file for elasticsearch indexing
    if path.exists(config_path):
        with open(file=config_path, encoding='utf-8') as p:
            index_config = json.load(p)

    # Load JSON with documents
    if path.exists(data_path):
        with open(data_path) as o:
            tweets = json.load(o)

    es = Elasticsearch(hosts=["http://localhost:9200"])

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name, body=index_config)

    # Index document with bulk function
    pprint("Indexing documents...")
    progress = tqdm.tqdm(unit="docs", total=len(tweets))
    successes = 0
    for ok, action in streaming_bulk(
        client=es, index=index_name, actions=genData(),
    ):
        progress.update(1)
        successes += ok
    pprint("Indexed %d/%d documents" % (successes, len(tweets)))
