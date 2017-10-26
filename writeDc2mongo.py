import gzip
import json
from lxml import etree
import re
from StringIO import StringIO
import pymongo
from utils import *

def parseline(line):
    body = json.loads(line)['body']
    input_str =body.encode('utf-8', 'ignore')
    data = StringIO(input_str)
    root = etree.parse(data)
    return root


def getidentifier(root):
    ides = root.xpath("//*[local-name()='identifier']")
    ret = ""
    p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')
    for ide in ides:
        if p.match(ide.text.strip()):#'datacite.org/schema/kernel' in ide.xpath('namespace-uri(.)'):
            ret = ide.text.strip()
            if (ret[:4] == "doi:"):
                ret = ret[4:]
            break
    return ret

def getlinebody(line):
    body = json.loads(line)['body']
    return body

client = pymongo.MongoClient("146.48.87.96", 27017)
db = client["DLIDB"]

collection = db["datacite"]



read = 0
with gzip.open('datacite_native.gz') as f:
    for line in f:
        read +=1

        if read%1000 == 0:
            print "read ", read
        try:
            body = getlinebody(line)
            root = parseline(line)
        except:
            continue

        ide = getidentifier(root)
        # print ide
        if ide == "":
            continue
        dic= {"body" : body,"_id" : ide}

        document = collection.find_one({"_id":ide})
        if document is None:
            collection.insert_one(dic)
        else:
            obody = document['body']
            if obody in body and obody != body:
                udic={"body":body}
                collection.update_one({'_id': ide}, {'$set': udic}, upsert=False)

