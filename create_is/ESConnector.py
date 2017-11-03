import sys

from elasticsearch import Elasticsearch
from elasticsearch_dsl import *

from utils import *


class ESConnector:
    def __init__(self, index_host, index_name=None):
        self.index_host = index_host
        self.client = Elasticsearch(hosts=index_host, timeout=30, max_retries=10, retry_on_timeout=True)
        if index_name is not None:
            self.index_name = index_name

    def pid_type(self,value):
        args = {'localIdentifier.id': value}
        query_type = Q('nested', path='localIdentifier', query=Q('bool', must=[Q('match', **args)]))
        s = Search(using=self.client).index(self.index_name).doc_type('object').query(query_type)
        try:
            response = s.execute()
            ret = []
            for hit in response.hits:
                ret.append(hit.__dict__['_d_'])
            return ret
        except:
            return []

    def item_by_id(self, id, type=None, start=None,doc_type='object'):
        q = Q('match', _id=id)
        s = Search(using=self.client, index=self.index_name).doc_type(doc_type).query(q)
        response = s.execute()

        for hit in response.hits:
            print hit.__dict__


    def write_document(self, pid, typology, abstracts, title, pid_type, provenance,  found=1):
        post = {"pid_type": pid_type, "resource_identifier": resource_identifier(pid, pid_type),
                "title": title, "type": typology, "abstracts": abstracts, "provenance": provenance,"found":found}
        try:
            self.client.index(index="pids", doc_type='known', id=pid, body=post)
        except Exception as e:
            print e


    def is_in(self, index_name, doc_type, pid):
        print "query index..."
        try:
            res = self.client.get(index=index_name, doc_type=doc_type, id=pid)
        except Exception as e:
            return None

        return res

# x = ESConnector(["192.168.100.108", "192.168.100.109", "192.168.100.106", "192.168.100.107"])
# # # print x.get('10.1016/j.epsl.2014.08.007')['found']
# # # print x.item_by_id("b45e0278664788c752a83a87274a75b6","resolved")
# # #print  x.pid_type("10.1594/pangaea.818322")
# print x.is_in('pids','known','10.1594/pangaea.818322')
# x = ESConnector(['192.168.100.70', '192.168.100.71', '192.168.100.72', '192.168.100.73'], 'dli')
#print x.pid_type('10.1594/pangaea.818322')