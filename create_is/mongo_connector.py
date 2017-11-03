import pymongo

from utils import *

class MongoConnector:
    def __init__(self, host):  # ,collection_name):
        self.client = pymongo.MongoClient(host, 27017)


    def write_document(self,db_name='CPROP',coll_name='datacite_prova',document={},merge = True):
        db = self.client[db_name]

        print db
        print "searching in mongo "
        gdoc = db[coll_name].find_one({"_id": document['_id']})

        if gdoc is None:
            print 'new mongo document'
            db[coll_name].insert_one(document)
        else:
            print 'document to be merged'
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

    def run(self):
        return null

# m = MongoConnector('localhost')
# print m.find_document(db_name='CPROP',coll_name='prova',id='10.3205/11gma285')
