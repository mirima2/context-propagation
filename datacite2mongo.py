import gzip
import json
from lxml import etree
from StringIO import StringIO
import re
from resolver import Resolvers
from mongo_connector import *
import sys

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
            ret = ide.text
            break
    return ret


def writeAbstractFile(info, resource):
    result_data = {}
    rels = []
    for item in info:
        rels.append([{item.get('relatedIdentifierType'):item.text},{'relation_type':item.get('relationType')}])

        ##   result = root.xpath("//*[local-name()='recordIdentifier']")[0].text
    return  [rels,{'resource_type':resource}]



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
    return {"rels":rels,"titles":titles,"abs":abss, "type": resource}
    #return []



def resolver(doi):

    # print "resolving... " , doi
   # print "dli..."
    provenance = []
    title, abstract, typology = r.resolverdli(doi)
    if title != []:
        provenance.append("dli")
    if abstract == '':
     #   print "datacite ..."
        dt, da, dp = r.resolvedatacite(doi)
        if title == []:
            title = dt
        abstract = da
        typology = dp
        if dt != []:
            provenance.append("datacite")

    # if abstract == '':
    #     if typology == '':
    #        print "crossref..."
        try:
             typology.lower()
        except:
            print "errore ", doi
            sys.exit()
        if typology == '' or typology.lower() != 'dataset' or typology.lower() != 'collection':
            dt,da,dp = r.resolvecrossref(doi)
            if title == []:
                title = dt
            if typology =='':
                typology = dp
            if dt != []:
                provenance.append("crossref")
    #        print "typology ", typology

        if typology == 'publication' or typology == '':
     #       print "openaire publications...."
            dt,da,dp = r.resolveopenaire(doi,'publication')
            if title == []:
                title = dt
            abstract = da
            if typology == '':
                typology = dp
            if dt != []:
                provenance.append("openaire")

        if typology =='dataset' or typology == '':
     #       print "openaire datasets...."
            dt,da,dp = r.resolveopenaire(doi,'dataset')
            if title == []:
                title = dt
            abstract = da
            if typology == '':
                typology = dp
            if dt != []:
                provenance.append("openaire")

    if abstract != '' or title != []:

        mongo_db.writeDocument(doi,typology,abstract,title,"doi", provenance)
        resolved =+1
        return True
    return False



def resolve(rels):
    p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')
    okrels=[]
    for rel in rels:
        id_type = rel['relatedIdentifierType'].strip().lower()
        if not id_type in resolving:
            continue
        pid = rel['relatedIdentifierValue'] if not rel['relatedIdentifierType'] is None else None
        # print pid
        # print id_type

        if pid is None:
            continue

        if id_type == 'handle' or id_type == 'url' or id_type == 'urn':
            #vedo se e' un doi
            m = re.search('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)',pid)
            # print "id_type ", id_type
            # print "pid ", pid

            if not m is None:
                id_type = doi
                pid = m.group()
                # print "new pid ", pid


        if id_type == 'doi':
            doi = pid
            if (doi[:4] == 'doi:'):
                doi = pid[4:]

            if (p.match(doi)):
                if doi in diff_dois:
                    if diff_dois[doi]==1:
                        okrels.append(rel)
                    continue

                if resolver(doi):
                    okrels.append(rel)

                    diff_dois[doi] = 1
                else:
                    diff_dois[doi] = 0
            continue
        if id_type == 'arxiv':
            # print "resolving arxiv ... " + pid
            if pid in diff_arxiv:
                if diff_arxiv[pid] == 1:
                    okrels.append(rel)
                continue
            title, abstract, typology = r.resolvearxiv(pid)
            if typology != '':
                okrels.append(rel)
                mongo_db.writeDocument(pid,typology,abstract,title,'arxiv',['arxiv'])
                diff_arxiv[pid]=1
            else:
                diff_arxiv[pid] = 0
            continue
        if id_type == 'pmid':
            # print "resolving pmid ... " + pid
            if pid in diff_pubmed:
                if diff_pubmed[pid] == 1:
                    okrels.append(rel)
                continue
            title, abstract, typology = r.resolvepubmed(pid)
            if typology != '':
                okrels.append(rel)
                mongo_db.writeDocument(pid,typology,abstract,title,'pmid',['pubmed'])
                diff_pubmed[pid] = 1
            else:
                diff_pubmed[pid] = 0
            continue

    return okrels


diff_dois = {}
diff_arxiv = {}
diff_pubmed = {}
mongo_db = MongoConnector("146.48.87.96", "DLIDB")
nodoi = 0
nodsetc = 0
altri = 0
resolved = 0
notjson = 0
resolving = ['doi','arxiv','handle','pmid','url','urn']
r = Resolvers()
count = 0

diff_dois = mongo_db.getDataciteDois()




with gzip.open('datacite_native.gz') as f:
    # for i in range (1000):
    #     print f.readline()
    # sys.exit()

    if count % 1000 == 0:
        print "parsed " , count , " lines"
        i = 0
    for line in f:
        # if i > 1000:
        #     break
        # i += 1
        try:
            root = parseline(line)
        except:
            #print "error on line " , line
            notjson += 1
            continue
        ide = getidentifier(root)
        #print ide
        if ide == "":
            nodoi += 1
            continue
        print ide
        ret = selectdocuments(root)

        if ret == []:
            nodsetc += 1
            continue
        altri +=1
        okrels = resolve(ret['rels'])
        #if okrels != []:
        ret['rels']=okrels
        resolved += len(okrels)
        #resolve(ret['relations'])
        mongo_db.writeDataciteDocument(ide,ret)
        diff_dois[ide] = 1

print "nodoi " , nodoi
print "nodsetc " , nodsetc
print "altri " , altri
print "resolved" ,  resolved
print "notjson " , notjson

