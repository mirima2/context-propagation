from ESConnector import ESConnector
from mongo_connector import MongoConnector
import urllib2
import json
from parser import *

class Resolvers:
    def __init__(self):
        self.dli_connector = ESConnector(['192.168.100.70', '192.168.100.71', '192.168.100.72', '192.168.100.73'], index_name='dli')
        self.openaire_index = "http://solr.openaire.eu:8983 /solr/DMF-index-openaire_shard1_replica1/select?q=%s&wt=json"
        self.crossref_baseurl = "http://api.crossref.org/works/"
        self.datacite_baseurl = "https://api.datacite.org/works/"
        self.openaire_publication_baseurl="http://api.openaire.eu/search/publications?doi="
        self.openaire_dataset_baseurl="http://api.openaire.eu/search/datasets?doi="
        self.arxiv_baseurl="http://export.arxiv.org/api/query?id_list="
        self.pubmed_baseurl="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id="
        self.mongo_db = MongoConnector("localhost")




    def resolve_dli(self,doi):
        print "resolve dli..."
        res = self.dli_connector.pid_type(doi)  # verifico se esiste risolto in dli

        title = []
        abstract = []
        typology = ''
        # try:

        if len(res) > 0:

            typology = res[0]['typology']
            if typology == 'publication' or typology == 'dataset':
                for dic in res:
                    try:
                        if title == []:
                            title = dic['title']
                        if dic['abstract'] != '':
                            abstract = [dic['abstract']]
                            break
                    except:
                        pass

        return title, abstract, typology

    def resolve_crossref_mongo(self,db, collection,pid):
        
        document = self.mongo_db.find_document(db_name=db,coll_name=collection,id=pid)
       
        if document is None:
            return [],[],''
        try:
            abst = document['abstract']
            abst = abst.replace("<jats:p>", '').replace("</jats:p>", '')
            abs = [abst]
        except:
            abs = []

        try:
            title = document['title']
        except:
            title = []

        try:
            type = document['type']
        except:
            type = ''

        return title, abs, type


    def resolve_doi(self,pid):
        provenance = []

        title, abstract, typology = self.resolve_dli(pid)
        if title != []:
            provenance.append("dli")
        if (abstract == [] ):
            print "resolve crossref..."
            dt, da, dp = self.resolve_crossref_mongo('crossRef','dump',pid)
            abstract = da
            if title == []:
                title = dt
            if typology == '':
                typology = dp
            if dt != []:
                provenance.append("crossref")

        if (abstract==[]):
            print "resolve openaire..."
            #       print "openaire publications...."
            dt, da, dp = self.resolve_openaire_index(pid)
            if title == []:
                title = dt
            abstract = da
            if typology == '':
                typology = dp
            if dt != []:
                provenance.append("openaire")

        return title, abstract, typology, provenance

    def connecttorestjson(self,url):
        exception = True
        count = 0
        rest_response = None
        while exception and count < 3:
            try:
                response = urllib2.urlopen(url)
                rest_response = json.loads(response.read())
                exception = False
            except:
                count += 1
        return exception,rest_response

    def resolve_openaire_index(self,doi):
        exception, response = self.connecttorestjson(self.openaire_index%("pid:" + doi))
        if exception:
            return [], [], ''
        else:
            if response['response']['numFound'] == 0:
                return [], [], ''
            else:
                docs = response['response']['docs'][0]['__result']
                print "sto andando in parser \n"
                return parseOpenaire(docs[0])


    def connecttorestxml(self,url):
        exception = True
        count = 0
        rest_response = None
        while exception and count < 3:
            try:
                response = urllib2.urlopen(url)
                rest_response = response.read()
                exception = False
            except:
                count += 1
        return exception,rest_response

    def resolve_arxiv(self,id):
        exception, response = self.connecttorestxml(self.arxiv_baseurl + id)
        if exception:
            return [], [], ''

        return parseArxiv(response)


    def resolve_pmed(self, id):
        exception, response = self.connecttorestxml(self.pubmed_baseurl + id)
        if exception:
            return [], [], ''

        return parsepubmed(response)

# r = Resolvers()
# print r.resolve_doi('10.1594/pangaea.818322')
