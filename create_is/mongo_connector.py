import pymongo

from utils import *
from threading import Thread

class MongoConnector(Thread):
    def __init__(self, host):  # ,collection_name):
        self.client = pymongo.MongoClient(host, 27017)
        self.wait_for_number = 20


    def write_document(self,db_name='CPROP',coll_name='datacite_prova',document={},merge = True):
        db = self.client[db_name]

        gdoc = db[coll_name].find_one({"_id": document['_id']})

        if gdoc is None:
            print 'new mongo document %s'%document['_id']
            db[coll_name].insert_one(document)
        else:
            print 'document to be merged %s'%document['_id']
            if merge:
                if 'datacite' in coll_name:
                    dic = updateIfDifferent(gdoc, document, "abstracts")
                    if dic != {}:
                        db[coll_name].update_one({'_id': document['_id']},
                                                                                    {'$set': dic}, upsert=False)


    def find_document(self, db_name='DLIDB', coll_name='datacite_relations',id=''):
        db=self.client[db_name]
        collection = db[coll_name]
        doc = collection.find_one({'_id':id})
        return doc

    def run(self, q):
        while True:
            action = q.get()

            if not type(action) is list:
                q.task_done()
                if (int(action)) == self.wait_for_number:
                    return
                else:
                    continue

            self.write_document(db_name = action[1], coll_name=action[2], document=action[0])
            q.task_done()

# m = MongoConnector('localhost')
# print m.find_document(db_name='CPROP',coll_name='prova',id='10.3205/11gma285')
