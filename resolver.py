from es_connector import DLIESConnector
import urllib2
import json
import parser
import sys

papers_list=['monograph','publication','journal-article','proceedings-article','book-chapter']

def connecttorestjson(url):
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

def connecttorestxml(url):
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

class Resolvers:
    def __init__(self):
        self.dli_connector = DLIESConnector('192.168.100.70', 'dli')
        self.openaire_index = "http://solr.openaire.eu:8983 /solr/DMF-index-openaire_shard1_replica1/select?q=%s&wt=json"
        self.crossref_baseurl = "http://api.crossref.org/works/"
        self.datacite_baseurl = "https://api.datacite.org/works/"
        self.openaire_publication_baseurl="http://api.openaire.eu/search/publications?doi="
        self.openaire_dataset_baseurl="http://api.openaire.eu/search/datasets?doi="
        self.arxiv_baseurl="http://export.arxiv.org/api/query?id_list="
        self.pubmed_baseurl="https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id="

    def resolverdli(self,doi):
        res = self.dli_connector.pid_type(doi)  # verifico se esiste risolto in dli
        title = []
        abstract = []
        typology = ''
        #try:

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

    def resolvecrossref(self,doi):
        exception, response = connecttorestjson(self.crossref_baseurl + doi)
        abs = []
        if exception:
            return [],[],''


        response_type = response['message']['type']

        if response_type in papers_list:
            try:
                abst = response['message']['abstract']
                abst = abst.replace("<jats:p>",'').replace("</jats:p>",'')
                abs = [abst]
            except:
                pass

        return response['message']['title'],abs,response_type

    def resolvedatacite(self,doi):

        exception, response = connecttorestjson(self.datacite_baseurl + doi)
        if exception:
            return [],[],''
        if response is None :
            return [],[],''

        if 'error' in response.keys():
            return [],[],''

        title = response['data']['attributes']['title']
        title = [] if title is None else [title]
        description = response['data']['attributes']['description']
        description = [] if description is None else [description]
        typology = response['data']['attributes']['resource-type-id']
        typology = '' if typology is None else typology

        return title , description, typology

    def resolveopenaireindex(self,doi):
        exception, response = connecttorestjson(self.openaire_index%("pid:" + doi))
        if exception:
            return [], [], ''
        else:
            if response['response']['numFound'] == 0:
                return [], [], ''
            else:
                docs = response['response']['docs'][0]['__result']
                print "sto andando in parser \n"
                return parser.parseOpenaire(docs[0])

    def resolveopenaire(self,doi, typology):
        exception, response = connecttorestjson(self.openaire_dataset_baseurl + doi + '&format=json') if typology.strip().lower() == 'dataset' else connecttorestjson(self.openaire_publication_baseurl + doi+ '&format=json')
        title = []
        description = []
        if exception:
       #     print "exception"
            return [],[],''

        if response['response']['results'] is None:
            return [], [], ''

        res = response['response']['results']['result']
        if not type(res) is list:
            res = [res]

        for elem in res:
            t = elem['metadata']['oaf:entity']['oaf:result']['title']
            if not type(t) is list:
                t=[t]

            if title == []:
                for e in t:
                    try:
                        title.append(e['$'])
                    except:
                        pass

            dex = elem['metadata']['oaf:entity']['oaf:result']['description']

            if dex != None:
                if not type(dex) is list:
                    dex = [dex]
                for d in dex:
                    try:
                        description.append(d['$'])
                    except:
                        pass
            if description != []:
                break

      #  print "ritorna..." , title, description, typology
        return title, description, typology

    def resolvearxiv(self, id):
        exception, response = connecttorestxml(self.arxiv_baseurl + id)
        if exception:
            return [],[],''

        return parser.parseArxiv(response)

    def resolvepubmed(self, id):
        exception, response = connecttorestxml(self.pubmed_baseurl + id)
        if exception:
            return [],[],''

        return parser.parsepubmed(response)

#
# r = Resolvers()
# print r.resolveopenaireindex("10.5281/zenodo.11082")
