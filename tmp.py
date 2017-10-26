import gzip
import json
from lxml import etree
from StringIO import StringIO
import re
import sys

from resolver import Resolvers

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
    return {"rels":rels,"titles":titles,"abs":abss, "type": resource}
    #return []

def resolve(rels,from_id):
    p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')
    okrels = []
    title = []
    abstract = []
    typology = ''
    provenance = []
    for rel in rels:
        try:
            id_type = rel['relatedIdentifierType'].strip().lower()
            relation_type = rel['relation_type'].strip().lower()

            if not id_type in resolving:
                continue
            pid = rel['relatedIdentifierValue']

            # print pid
            # print id_type

            if pid is None:
                continue
            pid = pid.strip().lower()

            if id_type == 'handle' or id_type == 'url' or id_type == 'urn':
                # vedo se e' un doi
                m = re.search('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)', pid)
                # print "id_type ", id_type
                # print "pid ", pid
                if m is None:
                    continue

                id_type = "doi"
                pid = m.group()
                    # print "new pid ", pid
            pid = pid.encode('utf-8', 'ignore')
            if id_type == 'doi':
                if (pid[:4] == 'doi:'):
                    pid = pid[4:]
            try:
                doc = resolved_dic[pid]
                title = doc['title']
                abstract = doc['abstracts']
                typology = doc['type']
                provenance = doc['provenance']
                resolved_pids[pid] = 1
                info = {"rels": [{"relatedIdentifierType": "DOI", "relatedIdentifierValue": from_id,
                                  "relation_type": mongo_db.get_inverse[relation_type]}], "titles": title, "abs": abstract,
                        "type": typology}  # if abstracts != [] else {"rels": [], "titles": title, "abs": [],"type":typology}
                mongo_db.writeDataciteDocument(pid,info,id_type)
            except:
                print "Not found pid " , pid , " of type ", id_type
                if id_type == 'doi':

                    doi = pid
                    if (doi[:4] == 'doi:'):
                        doi = pid[4:]

                    if (p.match(doi)):

                        if doi in resolved_pids:
                            if resolved_pids[doi] == 1:
                                okrels.append(rel)
                            continue

                        title, abstract, typology, provenance = resolver(doi,from_id)

                elif id_type == 'arxiv':

                    if pid in resolved_pids:
                        if resolved_pids[pid] == 1:
                            okrels.append(rel)
                        continue
                    title, abstract, typology = r.resolvearxiv(pid)
                    provenance.append('arxiv')

                elif id_type == 'pmid':

                    if pid in resolved_pids:
                        if resolved_pids[pid] == 1:
                            okrels.append(rel)
                        continue
                    title, abstract, typology = r.resolvepubmed(pid)
                    provenance.append('pubmed')

                if typology != '':
                    okrels.append(rel)

                    mongo_db.writeDocument(pid, typology, abstract, title, id_type, provenance,relation_type,from_id)
                    resolved_pids[pid] = 1
                else:

                    resolved_pids[pid] = 0

        except Exception as e:
            print "Problems resolving %s of type %s"%(pid,id_type)
    #print okrels
    return okrels

def resolver(self,doi,relation_type):

    # print "resolving... " , doi
    # print "dli..."
    # self.fout.write("resolving... " + doi)
    provenance = []
    title, abstract, typology = self.r.resolverdli(doi)
    if title != []:
        provenance.append("dli")
        self.fout.write("resolved by dli \n")
        self.fout.write("typology : " + typology + "\n")
    if abstract == []:
        #   print "datacite ..."
        self.fout.write("abstract = [] trying with datacite \n")
        dt, da, dp = self.r.resolvedatacite(doi)
        if title == []:
            title = dt
        abstract = da
        if typology == '':
            typology = dp
        if dt != []:
            provenance.append("datacite")
        try:
            typology = typology.lower()
        except:
            print "errore ", doi
            return False
            #sys.exit()
        self.fout.write("typology : " + typology + "\n")

        if (typology == '' or (typology != 'dataset' and typology != 'collection' and typology != 'publication') ):
            dt, da, dp = self.r.resolvecrossref(doi)
            if title == []:
                title = dt
            if typology == '':
                typology = dp
            if dt != []:
                provenance.append("crossref")
                #        print "typology ", typology

        if (typology == 'publication' or typology == '') and abstract==[]:
            #       print "openaire publications...."
            dt, da, dp = self.r.resolveopenaire(doi, 'publication')
            if title == []:
                title = dt
            abstract = da
            if typology == '':
                typology = dp
            if dt != []:
                provenance.append("openaire")

        if (typology == 'dataset' or typology == '' or typology == 'collection') and abstract == []:
            #       print "openaire datasets...."
            dt, da, dp = self.r.resolveopenaire(doi, 'dataset')
            if title == []:
                title = dt
            abstract = da
            if typology == '':
                typology = dp
            if dt != []:
                provenance.append("openaire")

    return title, abstract, typology, provenance



resolved_pids={}
mongo_db =  MongoConnector("146.48.87.96", "DLIDB")
resolved_dic = mongo_db.loadResolvedDocuments('resolved_documents')
read = 0
resolving = ['doi', 'arxiv', 'handle', 'pmid', 'url', 'urn']
r = Resolvers()
with gzip.open('datacite_native.gz') as f:
    for line in f:
        read +=1
        if read % 1000 == 0:
            print "parsed %i lines "%read
        try:
            root = parseline(line)
        except:

            continue

        ide = getidentifier(root)
        # print ide
        if ide == "":

            continue
        #print ide
        ret = selectdocuments(root)

        if ret == []:
            continue

        okrels = resolve(ret['rels'], ide)

        ret['rels'] = okrels

        mongo_db.writeDataciteDocument(ide, ret, "doi")
        resolved_pids[ide] = 1


    f.close()