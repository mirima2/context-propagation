from utils import *
import pymongo
import hashlib
import sys
import re



def modifycollection(collection, abs, title, rel,pid, type):
    if abs != []:
        if title != []:
            if rel != []:
                if type != '':
                    collection.update_one({'_id': pid}, {'$set': {"title": title, "abstracts": abs, "rels": rel, "type":type}},
                                      upsert=False)
                else:
                    collection.update_one({'_id': pid},
                                          {'$set': {"title": title, "abstracts": abs, "rels": rel}},
                                          upsert=False)
            else:
                if type != '':
                    collection.update_one({'_id': pid}, {'$set': {"title": title, "abstracts": abs, "type":type}},
                                      upsert=False)
                else:
                    collection.update_one({'_id': pid}, {'$set': {"title": title, "abstracts": abs}}, upsert=False)
        else:
            if rel != []:
                if type != '':
                    collection.update_one({'_id': pid}, {'$set': { "abstracts": abs, "rels": rel, "type":type}},
                                      upsert=False)
                else:
                    collection.update_one({'_id': pid}, {'$set': {"abstracts": abs, "rels": rel}}, upsert=False)
            else:
                if type != '':
                    collection.update_one({'_id': pid}, {'$set': { "abstracts": abs, "type":type}},
                                      upsert=False)
                else:
                    collection.update_one({'_id': pid}, {'$set': {"abstracts": abs}}, upsert=False)
    else:
        if title != []:
            if rel != []:
                if type != '':
                    collection.update_one({'_id': pid}, {'$set': {"title": title,  "rels": rel, "type":type}},
                                      upsert=False)
                else:
                    collection.update_one({'_id': pid},
                                          {'$set': {"title": title,  "rels": rel}},
                                          upsert=False)
            else:
                if type != '':
                    collection.update_one({'_id': pid}, {'$set': {"title": title, "type":type}},
                                      upsert=False)
                else:
                    collection.update_one({'_id': pid}, {'$set': {"title": title}}, upsert=False)
        else:
            if rel != []:
                if type != '':
                    collection.update_one({'_id': pid}, {'$set': { "rels": rel, "type":type}},
                                      upsert=False)
                else:
                    collection.update_one({'_id': pid}, {'$set': {"rels": rel}}, upsert=False)
            else:
                if type != '':
                    collection.update_one({'_id': pid}, {'$set': { "type":type}},
                                      upsert=False)



class MongoConnector:
    def __init__(self, host, db_name):  # ,collection_name):
        self.client = pymongo.MongoClient(host, 27017)
        self.db = self.client[db_name]

    # def writeDocument(self, pid, typology, abstracts, title, pid_type, provenance, relation_type, from_pid, coll_number = -1,newly_resolved = True):
    #     if newly_resolved:
    #         if coll_number == -1:
    #             collection = self.db.resolvedDocuments
    #         else:
    #             st = "resolvedDocuments_w_" + str(coll_number)
    #             collection = self.db[st]
    #         pid, found = toRemovePrefix(pid)
    #         pid, found = toStripId(pid)
    #         if collection.find_one({"_id":pid}) is not None:
    #             return
    #
    #         post = {"_id": pid, "pid": pid, "pid_type": pid_type, "resource_identifier": resource_identifier(pid, pid_type),
    #                 "title": title, "type": typology, "abstracts": abstracts, "provenance": provenance}
    #         collection.insert_one(post)
    #
    #     info = {"rels": [{"relatedIdentifierType":"DOI","relatedIdentifierValue":from_pid,"relation_type":inverse[relation_type.lower()]}], "title": title, "abs": abstracts,"type":typology} #if abstracts != [] else {"rels": [], "titles": title, "abs": [],"type":typology}
    #     self.writeDataciteDocument(pid, info, pid_type,coll_number)

    def writeDataciteDocument(self, pid, info, pid_type="doi", coll_number = -1):
        #print "writing document in collection number %i"%coll_number

        info['pid'] = pid
        info['pid_type'] = pid_type
        if coll_number == -1:
            collection = self.db.dataciteRelations
        else:
            st = "dataciteRelations_w_" + str(coll_number)
            #print "in collection %s"%st
            collection = self.db[st]

        self.mergeDatacite(collection, info)

    def getResolvedDois(self, coll_number = -1):
        dic = {}
        if coll_number == -1:
            collection = self.db.resolvedDocuments
        else:
            st = "resolvedDocuments_w_" + str(coll_number)
            collection = self.db[st]
        #collection = self.db.resolvedDocuments
        documents = collection.find({},{"_id":1})
        for d in documents:
            dic[d['_id']] = 1
        return dic

    def getDataciteDois(self, coll_number = -1):
        dic = {}
        #collection = self.db.dataciteRelations
        if coll_number == -1:
            collection = self.db.dataciteRelations
        else:
            st = "dataciteRelations_w_" + str(coll_number)
            collection = self.db[st]
        documents = collection.find({}, {"_id": 1})
        for d in documents:
            dic[d['_id']] = 1
        return dic

    def removeAllCollections(self):
        for i in range(100):
            self.db["resolvedDocuments_w_" + str(i)].drop()
            self.db["dataciteRelations_w_" + str(i)].drop()

    def mergeDatacite(self,collection,info):
        title = []
        abs = []
        type = ''
        pid = info['pid']
        pid_type = info['pid_type']
        document = collection.find_one({"_id": pid})
        if document is None:
            # info = {"relations":rels,"titles":titles,"descriptions":abss, "type": resource}
            post = {"_id": pid, "pid": pid, "pid_type": pid_type,
                    "resource_identifier": resource_identifier(pid, pid_type),
                    "rels": info["rels"],
                    "title": info["title"],
                    "abstracts": info['abs'],
                    "type": info['type']}
            collection.insert_one(post)

        else:
            dic = updateIfDifferent(document,info)
            if dic != {}:
                collection.update_one({'_id': pid}, {'$set': dic}, upsert=False)

    def loadResolvedDocuments(self,collection):
        documents = self.db[collection].find()
        dic = {}
        for document in documents:
            dic[document['_id']]=document
        return dic

    def mergeCollection(self,  collection_name,collection_out,drop_collection = False):
        print collection_name + "\n"
        documents = self.db[collection_name].find()

        for document in documents:

            gdoc = self.db[collection_out].find_one({'_id':document['_id']})
            if gdoc is None:
                self.db[collection_out].insert_one(document)
            else:
                if "datacite" in collection_name:
                    dic = updateIfDifferent(gdoc,document,"abstracts")
                    if dic != {}:
                        self.db[collection_out].update_one({'_id': document['_id']}, {'$set': dic}, upsert=False)
                # else:
                #     if self.db[collection_out].find_one({'_id':document['_id']}) is None:
                #         self.db[collection_out].insert_one(document)

        if drop_collection:
            self.db[collection_name].drop()


    def loadResolved(self,coll_number):
        return self.getDataciteDois(coll_number)

    def loadResolvedStore(self,coll_name):
        dic = {}
        documents = self.db[coll_name].find({},{"_id":1})
        for doc in documents:
            dic[doc['_id']] = 1
        return dic

    def selectDataciteSubset(self,coll_name):
        documents = self.db[coll_name].find({"$or":[{"type":"Dataset"},
                                                 {"type":"Collection"},
                                                 {"type":"publication"},
                                                  {"type":"dataset"},
                                                  {"type":"collection"}]}
                                          )
        newdocs = []
        tmp={"dataset":"60","publication":"50"}
        count = 0
        for document in documents:

            count +=1
            if count % 1000 == 0:
                print "elaborati %i documents" %count
            dic = {}
            type = document['type'].lower()
            if type == 'dataset' or type == 'collection':
                dic["type"]="dataset"
            else:
                dic["type"] = "publication"


            dic["id"] = tmp[dic["type"]] + "|" + document["resource_identifier"]
            dic["title"] = document["title"]
            dic["abstracts"] = document["abstracts"]
            dic["pid"] = document["_id"]
            dic["pid_type"]=document['pid_type']
            rel_dic={}
            rels = []
            try:
                rels = document["rels"]
                rels = rels + document['relations']
            except:
                pass

            ok_rels = []
            for rel in rels:
                pid = rel['relatedIdentifierValue']
                type = rel['relatedIdentifierType'].lower()
                if type == 'handle' or type == 'url' or type == 'urn':
                    m = re.search('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)', pid)
                    if not m is None:
                        pid = m.group()
                    else:
                        print "errore ", pid
                        sys.exit()

                #pid = pid[4:] if "doi:" == pid[:4] else pid
                pid = pid.strip().lower()
                neigh = self.db[coll_name].find({"_id": pid})

                if neigh is None:
                    print "error pid = ", pid, " for element ", document["_id"]
                    sys.exit()

                try:

                    neighbour = neigh[0]

                except:
                    for n in neigh:
                        print "n " , n
                        print
                    document
                    print "neighbour = ", neighbour
                    print "id : ", pid
                    input()
                    # try:
                    #     neigh = self.db[coll_name].find({"_id": pid.strip()})
                    #     neighbour = neigh[0]
                    # except:
                    #     print rel
                    #     print pid
                    #     sys.exit()

                if neighbour['_id'] in rel_dic:
                    continue
                relation = "rels:" + dic["type"]


                ntype = neighbour["type"].lower()
                if ntype == "":
                    continue
                if ntype == "dataset" or ntype =="collection":
                    relation += "_dataset_"
                    ntype = "dataset"
                else :
                    relation += "_publication_"
                if not ntype in tmp:
                    continue
                try:
                    relation += rel['relation_type'].lower() + ":" + tmp[ntype] + "|" + neighbour['resource_identifier']
                except:
                    print "pid : ",pid
                    sys.exit()

                rel_dic[neighbour["_id"]]=1

                ok_rels.append(relation)
                neighbour = None

            dic["rels"]=ok_rels

            self.db.is_new.insert_one(dic)
            #newdocs.append(dic)

        #self.db.is_new.insert_many(newdocs)
#
# x = MongoConnector("localhost", "DLIDB")
# x.removeAllCollections()
# tmp = x.getResolvedDois()
# tmp.update(x.getDataciteDois())
# print len(tmp)
#          # x = MongoConnector("146.48.87.96", "DLIDB", "resolvedDocuments")
            # x.writeDocument("10.1016/j.jviromet.2014.01.021","publication",[],["Subglacial bedforms reveal complex basal regime in a zone of paleo-ice stream convergence, Amundsen Sea Embayment, West Antarctica"])

# for e in tmp:
#     print e



