from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
from mongo_connector import *

def createelem(doi,st, p_id):
    field_key = ('doi','dex')
    field_value = (doi,st)
    return p_id, dict(zip(field_key,field_value))


def esaddbulk(inputfile):
    fin = open(inputfile)
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
    k=({'_index':'doidex','_type':'association','_id':p_id,'_source':es_doidex}for p_id, es_doidex in decode(fin))
    helpers.bulk(es,k)

def esaddbulk(prop_collection, start_collection):
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
    k = ({'_index': 'propagated_context_nested', '_type': 'association', '_id': p_id, '_source': es_doc} for p_id, es_doc in decode(prop_collection, start_collection))
    helpers.bulk(es, k)

def decode(prop_collection, start_collection):
    toes = mongo_db[prop_collection]
    is_new = mongo_db[start_collection]
    gdocs = is_new.find()

    for gdoc in gdocs:
        esdoc = toes.find_one({'id':gdoc['_id']})

        if esdoc is None:
            dic={"id":gdoc['_id'],"title":gdoc['titles'], "abs":gdoc['abstracts']}
        else:
            esdoc["title"]=gdoc['titles']
            esdoc['abs']=gdoc['abstracts']
            dic = esdoc
        yield dic['id'],dic

def decode(fin):
#    bulk = ""
    p_id = 0
    doi = None
    #fin = open(inputfile)
    #bulk = open('bulk.json','w')
   # es = Elasticsearch(hosts=[{'host':'localhost','port':9200}])
    try:
        for line in fin:
            line = line.replace('\n', " ")
#    line = line.replace('\t'," ")
#    line = line.replace('\r'," ")
            line = line.replace('"'," ")
            line = line.replace('\\'," ")
    
            if '\t$|$\t' in line:
                if not doi is None and not st.strip() == "":
                    p_id += 1
                    i1,i2 = createelem(doi,st.replace('\t'," "),p_id)
                    yield i1, i2#createelem(doi,st.replace('\t'," "),p_id)
                line = line.split('\t$|$\t')
                doi = line[0]
                st = line[1]
            else:
                st += line + " "
    except:
        print line, ' ', p_id
        sys.exit(0)
    yield createelem(doi,st.replace('\t'," "),p_id)

    fin.close()

mongo_db = MongoConnector("146.48.87.96", "DLIDB")
esaddbulk('toes', 'is_new')
