from mongo_connector import *
from resolver import Resolvers
import re
from Queue import Queue
from threading import Thread, Lock
import logging
from es_connector import DLIESConnector
import json


def found(obj):
    # TODO parsing del data per vedere se nel campo found c'e' un uno o uno zero
    data = json.loads(obj)
    return data['found'] == 1

class Worker(Thread):
    def __init__(self, ):  # ,collection_name):
        Thread.__init__(self)
       # self.resolved_pids = {}
        self.mongo_db = MongoConnector("localhost", "DLIDB")
        self.resolving = ['doi', 'arxiv', 'handle', 'pmid', 'url', 'urn']
        self.r = Resolvers()
        self.fout = None
        self.worker_number = -1
        self.number_of_workers = -1
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.ERROR)
        self.logger.addHandler(logging.FileHandler('/data/miriam/context-propagation/out/paralleldc2mongoerror.log'))
        self.resolved_pids = DLIESConnector('localhost', 'resolved_pids')


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

            if (typology == '' or (typology != 'dataset' and typology != 'collection') and abstract == [] ):
                dt, da, dp = self.r.resolvecrossref(doi)
                if title == []:
                    title = dt
                if typology == '':
                    typology = dp
                if dt != []:
                    provenance.append("crossref")
                    #        print "typology ", typology

            if (typology == '' and abstract==[]):
                #       print "openaire publications...."
                dt, da, dp = self.r.resolveopenaireindex(doi)
                if title == []:
                    title = dt
                abstract = da
                if typology == '':
                    typology = dp
                if dt != []:
                    provenance.append("openaire")

        return title, abstract, typology, provenance

    def verifyRelation(self,pid,rel,okrels,typology,abstract,title,id_type,provenance,relation_type,from_id):
        # if pid in self.resolved_pids:
        #     if self.resolved_pids[pid] == 1:
        #         rel['relatedIdentifierValue'] = pid
        #         okrels.append(rel)
        #         return okrels,True
        x = self.resolved_pids.get(pid,'resolved_pids','resolved')
        if x is None:
            return False
        if x['found']:
            rel['relatedIdentifierValue'] = pid
            okrels.append(rel)
            info = {"rels": [{"relatedIdentifierType": "DOI", "relatedIdentifierValue": from_id,
                              "relation_type": inverse[relation_type.lower()]}], "title": title, "abs": abstract,
                    "type": typology}
            self.mongo_db.writeDataciteDocument(pid, info, id_type, self.worker_number)


        return True



    def resolve(self,rels,from_id):
        p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')
        okrels = []
        title = []
        abstract = []
        typology = ''
        provenance = []

        for rel in rels:
            try:
                id_type = rel['relatedIdentifierType']
                if not id_type is None:
                    id_type = id_type.strip().lower()
                relation_type = rel['relation_type']
                if relation_type is not None:
                    relation_type = relation_type.strip().lower()

                self.fout.write("relation.relatedIdentifierType : %s \n" %id_type)
                self.fout.write("relation.relationType : %s \n" % relation_type)
                if not id_type in self.resolving:
                    self.fout.write("Id type %s not in the resolving set \n"%id_type)
                    continue
                pid = rel['relatedIdentifierValue']

                if pid is None:
                    self.fout.write("No pid provided \n")
                    continue

                if id_type == 'handle' or id_type == 'url' or id_type == 'urn':
                    # vedo se e' un doi
                    m = re.search('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)', pid)

                    if m is None:
                        continue

                    id_type = "doi"
                    rel['relatedIdentifierType']='doi'
                    pid = m.group()

                pid = pid.encode('utf-8', 'ignore')
                pid = pid.strip().lower()
                if id_type == 'doi':

                    if (pid[:4] == 'doi:'):
                        pid = pid[4:]

                    if (p.match(pid)):
                        try:
                            self.fout.write( "resolving doi %s...\n"%pid)
                        except:
                            self.fout.write("resolving doi ...")
                        already_resolved = self.verifyRelation(pid,rel,okrels,typology,abstract,title,id_type,provenance,relation_type,from_id)
                        if already_resolved:
                            continue
                        title, abstract, typology, provenance = self.resolver(pid,from_id)

                elif id_type == 'arxiv':
                    try:
                        self.fout.write( "resolving arxiv ... %s\n" %pid)
                    except:
                        self.out.write("resolving arxiv...\n")
                    already_resolved = self.verifyRelation(pid, rel, okrels, typology, abstract, title,
                                                                   id_type, provenance, relation_type, from_id)
                    if already_resolved:
                        continue
                    if(pid[:6] == 'arxiv:'):
                        pid = pid[6:]
                    title, abstract, typology = self.r.resolvearxiv(pid)
                    provenance.append('arxiv')

                elif id_type == 'pmid':
                    try:
                        self.fout.write( "resolving pmid ... %s \n" %pid)
                    except:
                        self.fout.write("resolving pmid ... %s \n")
                    already_resolved = self.verifyRelation(pid, rel, okrels, typology, abstract, title,
                                                                   id_type, provenance, relation_type, from_id)
                    if already_resolved:
                        continue
                    if(pid[:5] == "pmid:"):
                        pid = pid [5:]
                    title, abstract, typology = self.r.resolvepubmed(pid)
                    provenance.append('pubmed')

                if typology != '':
                    rel['relatedIdentifierValue']=pid
                    okrels.append(rel)
                    #self.fout.write("resolved pmid \n")
                    # self.fout.write("writing document to mongo \n")
                    # self.mongo_db.writeDocument(pid, typology, abstract, title, id_type, provenance,relation_type,from_id,self.worker_number)
                    self.resolved_pids.write_document(pid, typology, abstract, title, id_type, provenance)

                    info = {"rels": [{"relatedIdentifierType": "DOI", "relatedIdentifierValue": from_id,
                                      "relation_type": get_inverse(relation_type.lower())}], "title": title,
                            "abs": abstract,
                            "type": typology}  # if abstracts != [] else {"rels": [], "titles": title, "abs": [],"type":typology}
                    self.mongo_db.writeDataciteDocument(pid, info, id_type, self.worker_number)
                    #self.resolved_pids[pid] = 1
                else:
                    self.fout.write("not resolved pid=%s with pid type=%s\n"%(pid,id_type))
                    #self.resolved_pids[pid] = 0
                    self.resolved_pids.write_document(pid, typology, abstract, title, id_type, provenance,found=0)
            except Exception as e:
                self.logger.exception("Worker %i Problems resolving %s of type %s"%(self.worker_number,pid,id_type))
        #print okrels
        return okrels


    def worker(self,q,index,numberOfWorkers):
        self.worker_number=index
        self.number_of_workers=numberOfWorkers
        self.fout = open("/data/miriam/context-propagation/out/worker_%i.txt"%index,'a')

        count = 0
        while True:
            line = q.get()
            count +=1
            if not type(line) is list:
                if(int(line))<self.number_of_workers:
                    q.put(str(int(line) + 1))

                q.task_done()

                self.fout.write( 'Done Worker %i number %i'%(self.worker_number, count))
                return

            ret = line[1]
            ide = line[0]
            try:
                self.fout.write( "worker %i, ide %s\n"%(self.worker_number,ide))
            except:
                self.fout.write("worker %i, ide encoding problems "%self.worker_number)

            self.fout.flush()
            okrels = self.resolve(ret['rels'],ide)

            ret['rels']=okrels
            #resolved += len(okrels)
                #resolve(ret['relations'])
            #self, pid, info, pid_type="doi", coll_number = -1):
            self.fout.write("writing in mongo datacite relation \n\n")
            #self.fout.flush()
            self.mongo_db.writeDataciteDocument(ide,ret,"doi",self.worker_number)
            self.resolved_pids.write_document(ide, ret['type'], ret['abs'], ret['title'],ret['pid_type'], ['datacite'])


            q.task_done()
