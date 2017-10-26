import pymongo
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from utils import *

def es_add_bulk():
    es = Elasticsearch(hosts=[{'host':'localhost','port':9200}])
    k=({'_index':'resolved_pids','_type':'resolved','_id':id,'_source':es_doidex, 'timeout':30} for id, es_doidex in generate_data())
    helpers.bulk(es,k)


# def generate_data():
#     i = 0
#     client = pymongo.MongoClient('localhost', 27017)
#     db = client['DLIDB']
#     collection = db['resolved_documents']
#     documents = collection.find()
#
#     for d in documents:
#             i +=1
#             if i % 1000 == 0:
#                 print "added %i "%i
#             del d['_id']
#             d['found'] = 1
#             yield d['pid'], d

def generate_data():
    i = 0
    client = pymongo.MongoClient('localhost', 27017)
    db = client['DLIDB']
    collection = db['datacite_relations']
    documents = collection.find()

    for info in documents:
            i +=1
            if i % 1000 == 0:
                print "added %i "%i
            pid = info['pid']
            pid_type = info['pid_type']
            post = {"pid": pid, "pid_type": pid_type,
                    "resource_identifier": resource_identifier(pid, pid_type),
                    "title": info["title"],
                    "abstracts": info['abstracts'],
                    "type": info['type'],
                    "provenance":['datacite'],
                    "found":1}
            yield pid, post

es_add_bulk()
