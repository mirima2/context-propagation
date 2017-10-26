from lxml import etree
from StringIO import StringIO
import re
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import urllib2
from utils import *

base_url = 'https://oai.datacite.org/oai?verb=ListRecords'
resumption_token = ''

def es_add_bulk():
    
    es = Elasticsearch(hosts=[{'host':'192.168.100.108','port':9200}])
    k=({'_index':'pids','_type':'knowb','_id':p_id,'_source':es_doidex}for p_id, es_doidex in get_datacite_subset())
    helpers.bulk(es,k)

def get_datacite_subset(from_date = 'all'):
	url = base_url + '&metadataPrefix=datacite'
        print url
	if from_date != 'all':
		url += '&from' + from_date
	
	i = 0
	while True:
		resp = urllib2.urlopen(url)
		data = StringIO(resp.read())
		root = etree.parse(data)
		rt = root.xpath("//*[local-name()='resumptionToken']")
		if rt is None:
		    resumption_token=''
                else:
                    resumption_token = rt[0].text
		meta = root.xpath("//*[local-name()='metadata']")
		for m in meta:
			i+=1
			if i%100000 == 0:
				print "Parsed %i"%i
			ides = m.xpath(".//*[local-name()='identifier']")
			pid = ''
		    
			p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')
			for ide in ides:
				if ide is None:
					continue
				ide = ide.text.strip().lower()
				ma = p.match(ide)
				if not ma is None:#'datacite.org/schema/kernel' in ide.xpath('namespace-uri(.)'):
					pid = ma.group()
					break
			if pid == '':
				continue

			result = m.xpath(".//*[local-name()='resourceType']")
			try:
				resource = result[0].get('resourceTypeGeneral')
			except:
				resource = ''
				

			rel_id_result = m.xpath(".//*[local-name()='relatedIdentifier']")
			abss = []
			titles = []
			# rels = []
			result = m.xpath(".//*[local-name()='description']")
			for description in result:
				description_type = description.get('descriptionType')
				if description_type == 'Abstract':
					abss.append(description.text)
			result = m.xpath(".//*[local-name()='title']")
			for title in result:
				titles.append(title.text)
			# for item in rel_id_result:
			# 	rels.append({"relatedIdentifierType":item.get('relatedIdentifierType'),"relatedIdentifierValue":item.text,"relation_type":item.get('relationType')})

			# yield pid, {"rels":rels,"title":titles,"abstract":abss, "resource_type": resource.lower(),found:1}
			yield pid,{'title':titles,'abstracts':abss,
			           'pid_type':"doi",'type':resource.lower(),
			           'provenance':['datacite'],'resource_identifier':resource_identifier(pid,"doi"),'found':1}

		if resumption_token == '':
			break
		url = base_url + "&resumptionToken=" + resumption_token

es_add_bulk()

