import re
from threading import Thread

from ESConnector import *
from mongo_connector import MongoConnector
from resolver import Resolvers
from utils import *



resolving = ['doi', 'arxiv', 'handle', 'pmid', 'url', 'urn']


class Builder(Thread):
    def __init__(self, ):
        Thread.__init__(self)
        self.fout = None
        self.worker_number = -1
        self.number_of_workers = -1
        self.es = ESConnector(["192.168.100.108", "192.168.100.109", "192.168.100.106", "192.168.100.107"])
        self.r = Resolvers()
        self.mongo_db = MongoConnector("localhost")


    def is_resolved(self,pid):
        print "verifying if already resolved..."
        x = self.es.is_in('pids','known',pid)

        if x is None:
            return -1
        print "found something"
        print x
        return x

    def run(self ,q ,index ,numberOfWorkers):
        self. worker_number =index
        self. number_of_workers =numberOfWorkers


        if numberOfWorkers > 1 :
            self.fout = open( "/data/miriam/context-propagation/out/worker_%i.txt" %index ,'a')
        else:
            self.fout=open("../test.out",'w')
        count = 0
        while True:
            line = q.get()
            count +=1
            if not type(line) is dict:
                if (int(line)) < self.number_of_workers:
                    q.put(str(int(line) + 1))

                q.task_done()

                self.fout.write('Done Worker %i number %i' % (self.worker_number, count))
                return
            ide = line['pid']

            print "\n\n\n IDENTIFIER %s"%ide

            try:
                self.fout.write("worker %i, ide %s\n" % (self.worker_number, ide))
            except:
                self.fout.write("worker %i, ide encoding problems " % self.worker_number)

            self.fout.flush()

            p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')
            okrels = []
            title = []
            abstract = []
            typology = ''
            provenance = []

            for rel in line['rels']:
                try:
                    id_type = rel['relatedIdentifierType']
                    if not id_type is None:
                        id_type = id_type.strip().lower()
                    relation_type = rel['relation_type']
                    if relation_type is not None:
                        relation_type = relation_type.strip().lower()
                    print "Relation type %s"%relation_type
                    if not id_type in resolving:
                        self.fout.write("Id type %s not in the resolving set \n" % id_type)
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
                        rel['relatedIdentifierType'] = 'doi'
                        pid = m.group()

                    pid = pid.encode('utf-8', 'ignore')
                    pid = pid.strip().lower()
                    pid, found = toRemovePrefix(pid, id_type + ':')
                    print "Relation pid ", pid
                    x = self.is_resolved(pid)
                    already_resolved = x['found']
                    print 'already resolved = ', already_resolved

                    if already_resolved == 0: #ho provato a risolverlo e non ho trovato nulla
                        continue

                    if already_resolved == -1: #non risolto
                        if id_type == 'doi':
                            title, abstract, typology, provenance = self.r.resolve_doi(pid)
                        elif id_type == 'arxiv':
                            title, abstract, typology, provenance = self.r.resolve_arxiv(pid)
                        else:
                            title, abstract, typology, provenance = self.r.resolve_pmed(pid)
                        print "newly resolved pid %s"%pid
                        print "typology = %s"%typology
                        print "title = " , title
                        print "abstract = " , abstract
                        self.es.write_document(pid, typology, abstract, title, id_type, provenance,found=(typology!=''))
                        print "after"

                    if already_resolved or typology != '': #o l'ho risolto o era gia' risolto con esito positivo
                        print "qui"
                        rel['relatedIdentifierValue'] = pid
                        okrels.append(rel)
                        info={}
                        if already_resolved:
                            print "here"
                            print relation_type.lower()
                            try:
                                info = {"rels": [{
                                        "relatedIdentifierType": "DOI",
                                        "relatedIdentifierValue": ide,
                                        "relation_type": inverse[relation_type.lower()]
                                        }],
                                    "title": x['_source']['title'],
                                    "abstracts": x['_source']['abstracts'],
                                    "type": x['_source']['type']
                                }
                            except Exception as e:
                                print e
                                input()
                        else:
                            info = {"rels": [{"relatedIdentifierType": "DOI", "relatedIdentifierValue": ide,
                                          "relation_type": inverse[relation_type.lower()]}], "title": title, "abstracts": abstract,
                                          "type": typology}
                        #def write_document(self,db_name='DLIDB',coll_name='datacite_relations',document={},merge = True):
                        print "here"
                        print "info : " , info
                        info['_id'] = pid
                        print "here"
                        info['pid_type']=id_type
                        print "info = ", info
                        if numberOfWorkers > 1:
                            self.mongo_db.write_document(coll_name='datacite_relations_w%i'%self.worker_number, document=info)
                        else:
                            print 'writing in mongo'
                            self.mongo_db.write_document(coll_name='prova',
                                                         document=info)
                            print "new datacite document from relation --> newly resolved pid"
                            print info


                except Exception as e:
                    print  "Worker %i Problems resolving %s of type %s" % (self.worker_number, pid, id_type)
                    print e


            self.fout.write("writing in mongo datacite relation \n\n")
            info = {"_id":ide,"pid_type":'doi',"rels":okrels,'title':line['title'],'abstracts':line['abstracts'],'type':line['type']}

            if numberOfWorkers > 1:
                self.mongo_db.write_document(coll_name='datacite_relations_w%i'%self.worker_number,document = info)

                #self.es.write_document(ide, line['type'], line['abstracts'], line['title'], line['pid_type'], ['datacite'])
            else:
                print "New datacite document"
                self.mongo_db.write_document(coll_name='prova', document=info)
                print info
                #input()


            q.task_done()
