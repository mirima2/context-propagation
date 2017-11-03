import gzip
import json
import re
import sys
from StringIO import StringIO

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from lxml import etree

from create_is.utils import *

finished = False

def es_add_bulk(file = '/data/miriam/context-propagation/datacite_native.gz'):
    es = Elasticsearch(hosts=["192.168.100.108", "192.168.100.109", "192.168.100.106", "192.168.100.107"])
    while not finished:
        try:
            for item in helpers.streaming_bulk(es, get_datacite(file), raise_on_error=False):
                pass
        except Exception as e:
            print e
            pass
                      
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
        m = p.search(ide)
        if not m is None:#'datacite.org/schema/kernel' in ide.xpath('namespace-uri(.)'):
            ret = m.group()
            if (ret[:4] == "doi:"):
                ret = ret[4:]
            break
    return ret


def selectdocuments(root):
    result = root.xpath("//*[local-name()='resourceType']")
    if len(result) == 0:
        return {}
    resource = result[0].get('resourceTypeGeneral')
   # rel_id_result = root.xpath("//*[local-name()='relatedIdentifier']")
    abss = []
    titles = []
   # rels = []
    result = root.xpath("//*[local-name()='description']")
    for description in result:
        description_type = description.get('descriptionType')
        if description_type == 'Abstract':
            abss.append(description.text)
    result = root.xpath("//*[local-name()='title']")
    for title in result:
        titles.append(title.text)
    #for item in rel_id_result:
     #   rels.append({"relatedIdentifierType":item.get('relatedIdentifierType'),"relatedIdentifierValue":item.text,"relation_type":item.get('relationType')})
    return {"title":titles,"abstracts":abss, "type": resource.lower()}


def get_datacite(file):
    global finished

    read = 0
    with gzip.open(file) as f:
        for line in f:

            read += 1
            if read%1000==0:
                print "parsed %i"%read
           
            try:
                root = parseline(line)
            except:
                continue

            ide = getidentifier(root)
            if ide == "":          
                continue
            res = selectdocuments(root)

            if res == {}:
                continue
            res['pid_type'] = 'doi'
            res['provenance']=['datacite']
            res['resource_identifier']=resource_identifier(ide,"doi")
            res['found']=1
            yield {'_index':'pids','_type':'known','_id':ide,'_source':res}

    finished = True

es_add_bulk(sys.argv[1])
# def prova():
#     gen = get_datacite()
#     for i in range (5000):
#         print gen.next()
#         input()
# prova()
# def lastLine():
#     f = gzip.open('/data/miriam/context-propagation/datacite_native.gz')
#     i=0
#     for line in f:
#         i+=1
#         if i%100000==0:
#             print "processed %i. Associated line %s"%(i,line)
#
#
#     print "last line = %s"%line
#
# lastLine()