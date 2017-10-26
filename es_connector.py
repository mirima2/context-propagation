from elasticsearch import Elasticsearch
from elasticsearch_dsl import *
from utils import *


class DLIESConnector:
    def __init__(self, index_host, index_name=None):
        self.index_host = index_host
        self.client = Elasticsearch(hosts=[index_host],timeout=30, max_retries=10, retry_on_timeout=True)
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
        post = {"pid": pid, "pid_type": pid_type, "resource_identifier": resource_identifier(pid, pid_type),
                "title": title, "type": typology, "abstracts": abstracts, "provenance": provenance,"found":found}
        res = self.client.index(index="resolved_pids", doc_type='resolved', id=pid, body=post)


    def get(self, pid, index_name, doc_type):
        try:
            res = self.client.get(index=index_name, doc_type=doc_type, id=pid)
        except:
            return None
        return res
#
# x = DLIESConnector('localhost', 'resolved_pids')
# print x.get('10.1016/j.epsl.2014.08.007')['found']
# print x.item_by_id("b45e0278664788c752a83a87274a75b6","resolved")
# print  x.pid_type("10.1016/j.epsl.2014.08.007")
