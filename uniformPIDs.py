import pymongo
import re
import hashlib
from utils import *


client = pymongo.MongoClient("146.48.87.96", 27017)
db = client["DLIDB"]

documents = db["gatherer"].find()

count = 0

for document in documents:
    oid = document["_id"]
    id, found1 = toRemovePrefix(oid)
    try:
        id, found = toStripId(id)
    except:
        print id
        input()

    if found or found1:
        print "oid: %s  new id: %s"%(oid,id)
        document["_id"]=id
        document["pid"] = id
        document['resource_identifier']=resource_identifier(id,document['pid_type'])
        replace = True
    nr = []

    found = False
    found1 = False
    for rel in document['rels']:
        oneigh = rel['relatedIdentifierValue']
        neigh, found1 = toRemovePrefix(oneigh)
        neigh, found = toStripId(neigh)

        if found or found1:
            rel['relatedIdentifierValue']=neigh
            print "oneigh_id: %s  new neigh_id: %s" % (oneigh, neigh)

        nr.append(rel)

    document['rels']=nr


    doc = db['prova'].find_one({"_id":document["_id"]})
    if(doc is None):
        db["prova"].insert_one(document)
    else:
        dic = updateIfDifferent(doc, document, "abstracts")
        if dic != {}:
            db["prova"].update_one({'_id': document['_id']}, {'$set': dic}, upsert=False)
    # db["gatherer"].remove({"_id":oid})


print count