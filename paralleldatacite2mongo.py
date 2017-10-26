import gzip
import json
from lxml import etree
from StringIO import StringIO
import re
import sys
from Worker import *

import logging
from Queue import Queue
from threading import Thread, Lock
from mongo_connector import *

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
        if ide is None:
            continue
        ide = ide.text.strip().lower()
        m = p.match(ide)
        if not m is None:#'datacite.org/schema/kernel' in ide.xpath('namespace-uri(.)'):
            ret = m.group()
            if (ret[:4] == "doi:"):
                ret = ret[4:]
            break
    return ret


def selectdocuments(root):
    result = root.xpath("//*[local-name()='resourceType']")
    if len(result) == 0:
        return []
    resource = result[0].get('resourceTypeGeneral')
    rel_id_result = root.xpath("//*[local-name()='relatedIdentifier']")
    abss = []
    titles = []
    rels = []
    result = root.xpath("//*[local-name()='description']")
    for description in result:
        description_type = description.get('descriptionType')
        if description_type == 'Abstract':
            abss.append(description.text)
    result = root.xpath("//*[local-name()='title']")
    for title in result:
        titles.append(title.text)
    for item in rel_id_result:
        rels.append({"relatedIdentifierType":item.get('relatedIdentifierType'),"relatedIdentifierValue":item.text,"relation_type":item.get('relationType')})
    return {"rels":rels,"title":titles,"abs":abss, "type": resource.lower()}

def mergeStores(collection_prefix,stores_number, out_collection_name):
    mongo_db = MongoConnector("localhost", "DLIDB")
    for i in range(stores_number):
        collection_name = collection_prefix + "_w_" + str(i)
        mongo_db.mergeCollection(collection_name,out_collection_name)

def selectDataciteSubset():
    mongo_db = MongoConnector("localhost", "DLIDB")
    mongo_db.selectDataciteSubset('gatherer')

nodoi = 0
nodsetc = 0
notjson = 0

worker_number = 20

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')#,filename='paralleldc2mongo.log',level=logging.DEBUG)
fhi = logging.FileHandler('/data/miriam/context-propagation/out/paralleldatacite2mongoinfo.log')
info = logging.getLogger(__name__)
info.setLevel(logging.INFO)
info.addHandler(fhi)


###################################################
info.info('Started')

q = Queue(maxsize=worker_number)
info.info('Creating the workers ')
for i in range(worker_number):
    wr = Worker()
    w = Thread(target=wr.worker,args=(q,i,worker_number,))
    w.daemon = True
    w.start()

info.info('...done')
read = 0
added = 0
notjsonout = open("/data/miriam/context-propagation/out/notjson.txt",'w')
nodoiout = open("/data/miriam/context-propagation/out/nodoi.txt",'w')
nodsout = open("/data/miriam/context-propagation/out/nodsetc.txt",'w')

with gzip.open('/data/miriam/context-propagation/datacite_native.gz') as f:
    for line in f:
    # for i in range(5000):
        line = f.readline()
        read += 1
        if read%1000==0:
            info.info("Read %i"%read)
            info.info("nodoi %i"%nodoi)
            info.info("nodsetc %i"%nodsetc)
            info.info("notjson %i"%notjson)

        try:
            root = parseline(line)
        except:
            notjson += 1
            notjsonout.write(line + "\n")
            continue

        ide = getidentifier(root)
        if ide == "":
            nodoi += 1
            nodoiout.write(line + "\n")
            continue
        ret = selectdocuments(root)

        if ret == []:
            nodsetc += 1
            nodsout.write(line + "\n")
            continue

        q.put([ide.strip().lower(),ret])
        added +=1
        if (added % 1000 ==0):
            info.info( "Added %i"%added)


    f.close()
    notjsonout.close()
    nodoiout.close()
    nodsout.close()
    info.info( "out of for ")

q.put('1')
info.info( "waiting for completion")
q.join()

info.info( "nodoi %i"%nodoi)
info.info("nodsetc %i"%nodsetc)
info.info("notjson %i"%notjson)
##########################################################
#info.info("merging worker stores")
#mergeStores("dataciteRelations",worker_number,'gatherer')
#mergeStores("resolvedDocuments",worker_number,'resolved_documents')

#info.info("Selecting the datacite relation subset regarding dataset, collection and publications...")
#selectDataciteSubset()
info.info('Finished')
