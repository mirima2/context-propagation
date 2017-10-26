#-*- coding: latin-1 -*-
import json
from lxml import etree
from StringIO import StringIO
import sys
import re

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

def updateinfo(root):
    global resource_type, definition_type,relation_type
    global dataset_relation_type,abs_semantic_relationships
    global noabs_semantic_relationships
    global sem_rel_numb, dataset_sem_rel_numb,collection_sem_rel_numb

    rel_id_result = root.xpath("//*[local-name()='relatedIdentifier']")
    if len(rel_id_result) > 0: #ho una relazione semantica      
        sem_rel_numb += 1
        updateRelationType(relation_type,rel_id_result)        
                
    result = root.xpath("//*[local-name()='resourceType']")
    if len(result) == 0:
        resource = "Undefined"

    else:
        resource = result[0].get('resourceTypeGeneral')

    try:

        resource_type[resource] += 1
    except:

        resource_type[resource] = 1
 
    if resource == "Dataset" or resource == "Collection":

        
        if len(rel_id_result)>0:
            if resource == "Dataset":
                dataset_sem_rel_numb += 1
            else:
                collection_sem_rel_numb += 1
                
            updateRelationType(dataset_relation_type,rel_id_result)
            
        result = root.xpath("//*[local-name()='description']")
        foundabs = False
        if len(result)>0:#esiste una descrizione per la risorsa
            foundabs = False
            alreadyfound = []
            for description in result:
                description_type = description.get('descriptionType')
                if not description_type in alreadyfound:#conto ogni tipo di descrizione associata alla stessa entry una sola volte indipendentemente dal numero di volte che appare 
                    updateAssociationValues(definition_type,resource,description_type)
                    alreadyfound.append(description_type)

                if description_type == 'Abstract':
                    foundabs = True  
   
 
        toupdate = abs_semantic_relationships if foundabs else noabs_semantic_relationships
        if(len(rel_id_result)>0):
            for item in rel_id_result:
                updateAssociationValues(toupdate,resource,item.get('relationType'))
 #               relationsinfofile.write("DOI = %s , resource = %s , found abstract = %r , relation_type = %s \n"%(ide,resource,foundabs,item.get('relationType')))
       
        toupdate = abstractfile if foundabs else noabstractfile
        writeAbstractFile(toupdate,rel_id_result,ide,resource)
        

def updateRelationType(dic,relation):
    for item in relation:
        rel = item.get("relationType")
        try:
            dic[rel] += 1
        except:
            dic[rel] = 1

def writeAbstractFile(afile, info, ide, resource):           
    result_data = {}
    rels = []
    for item in info:
        rels.append([{item.get('relatedIdentifierType'):item.text},{'relation_type':item.get('relationType')}])

 ##   result = root.xpath("//*[local-name()='recordIdentifier']")[0].text
    result_data[ide] =[rels,{'resource_type':resource}]
    afile.write(json.dumps(result_data) + '\n')

def updateAssociationValues(dic,v1,v2):
    try:
        e = dic[v1]
        try:
            e[v2] += 1
        except:
            e[v2] = 1
    except:
        dic[v1]={v2:1}

def writeInfo():
    global resource_type, definition_type,relation_type
    global dataset_relation_type, abs_semantic_relationships
    global noabs_semantic_relationships
    global sem_rel_numb, dataset_sem_rel_numb,collection_sem_rel_numb
                
    rfile = open('dataciteGeneralInfo.txt','w')
    otherinfo = open('dataciteGeneralInfoExtended.txt','w')
    
    rfile.write("Resource Type \n")
    for key in resource_type:
        rfile.write("%s %i \n" %(key,resource_type[key]))

    rfile.write('\n\n')
    
    rfile.write('\n\nNumber of items with relatedIdentifier : %i \n'%(sem_rel_numb ))
    rfile.write("Semantic Relationships Over All the entries\n")
    for key in relation_type:
        rfile.write("%s = %i \n"%(key,relation_type[key]))
        
    rfile.write('\nDataset + Collection description type \n')
    for key in definition_type:
        rfile.write(key + '\n')
        for item in definition_type[key]:
            rfile.write("\t%s = %i \n"%(item,definition_type[key][item]))

    rfile.write("\n\nNumber of Datasets with semantic relationships = %i  Number of Collection with Semantic Relationships = %i \n\t" % (dataset_sem_rel_numb,collection_sem_rel_numb))
    for key in dataset_relation_type:
        rfile.write("%s = %i \n\t"%(key,dataset_relation_type[key]))

    rfile.write("\n\n")
    for key in abs_semantic_relationships:
        rfile.write("\n\n %s with abstract semantic relationships\n"%(key))
        for item in abs_semantic_relationships[key]:
            rfile.write("\t%s = %i\n"%(item,abs_semantic_relationships[key][item]))

    rfile.write("\n\n")
    for key in noabs_semantic_relationships:
        rfile.write("\n\n %s without abstract semantic relationships\n"%(key))
        for item in noabs_semantic_relationships[key]:
            rfile.write("\t%s = %i\n"%(item,noabs_semantic_relationships[key][item]))
        

    rfile.close()

#prima passata per collezionare le entries doppie
f = open('datacite.json')
f_out = open('not_doi.json','w')
dic = {}
not_doi = 0
parsed = 0
for line in f:
    root = parseline(line)
    ide = getidentifier(root)
    parsed += 1
    if(parsed % 1000 ==0):
        print "parsed %d"%parsed
        
    if ide == "":
        not_doi += 1
        f_out.write(line)
        continue
    try:
        dic[ide] += 1
    except:
        dic[ide] = 1
        
f.close()
f_out.close()
print "Removing entries from dictionary..."
for key in dic.keys():
    if dic[key] == 1:
        del dic[key]

print len(dic)
print not_doi


dic1 = {}
#seconda passata come dataciteStats.py solo che se non c'è il DOI non si considera
f = open('datacite.json')

abstractfile = open('abstractout_v2.json','w')
noabstractfile = open('noabstractout_v2.json','w')
parsed = 0
resource_type = {}
definition_type = {}
relation_type = {}
dataset_relation_type={}
abs_semantic_relationships = {}
noabs_semantic_relationships = {}
count=0

sem_rel_numb = 0
dataset_sem_rel_numb = 0
collection_sem_rel_numb = 0

for line in f:
    root = parseline(line)
    ide = getidentifier(root)
    if ide == "":
        continue
    try:
        dic[ide]
        try:
            #this is to handle double entries for the same DOI
            l = dic1[ide]
            if len(line) > len(l):
                dic1[ide] = line
        except:
            dic1[ide] = line
        continue
    except:
        pass
    
    updateinfo(root)

    parsed += 1

    if (parsed%1000 == 0):
        print "parsed " , parsed
        abstractfile.flush()
        noabstractfile.flush()

print "Last %d entries to parse "%len(dic1)
for entry in dic1:
    root = parseline(dic1[entry])
    ide = entry
    updateinfo(root)

    
f.close()
abstractfile.close()
noabstractfile.close()
#relationsinfofile.close()
#print (" --- Execution time %s --- " %(time.time()-start_time))
writeInfo()

