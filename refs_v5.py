#-*- coding: latin-1 -*-

import sys
import json
import urllib2
import re
import time

from Queue import Queue
from threading import Thread, Lock


lines_with_no_references = 0
number_of_references_to_papers = 0
total_number_of_lines = 0
lines_with_refs_to_paper = 0
references = {}
lines_with_no_response = 0

f = open('jsondump.json')#once jsondump.json is abstractout.json and once it is no_abstractout.json The dump file created at the end of the execution (dump.json)  will be renamed dump_abs.json if obtained by the abstractout.json and dump_noabs.json if obtained from the noabstractout.json files respectively
p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')

def connecttorest(url):
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
 
def responsetype(doi,ferr):
    response_type = ""
    exception, dli_response = connecttorest("http://node0-p-dli.d4science.org:8983/solr/DLIF-index-cleaned/select?q=localidentifier:%22" + doi + "%22&wt=json")
          
    if exception:
        ferr.write('dli ex ' + doi + "\n")# + line)                                                                                                                     
    if exception or dli_response['response']['docs'] == []:
        exception, crossref_response = connecttorest("http://api.crossref.org/works/" + doi)
        if exception:
            ferr.write('crossref ex %s \n'%doi)
        else:
            response_type = crossref_response['message']['type']
    else:
        dnet_id = dli_response['response']['docs'][0]['dnetresourceidentifier']
        exception,dnet_response = connecttorest("http://node0-p-dli.d4science.org:8080/dli/mvc/ui/dli/dli_objectStore/retrieveObject.do?id=" + dnet_id)
        if exception:
            ferr.write('dnet ex ' + doi + '\n')
        else:
            try:
                response_type = dnet_response['type']
                exception = False
            except:
                print doi
                exception = True
        if exception:
            exception, crossref_response = connecttorest("http://api.crossref.org/works/" + doi)
            if exception:
                ferr.write('crossref ex %s \n'%doi)
            else:
                response_type = crossref_response['message']['type']

    return response_type

def worker(q,index,worker_number):
    no_refs = 0 #numero di linee senza riferimenti
    refs = 0 #numero di riferimenti a paper diversi trovati
    tot_doc = 0 #numero totale di documenti
    p_refs = 0 #numero di linee con almeno un riferimento
    not_responding = 0 #numero di elementi per cui non ho ottenuto risposta nè da dli nè da crossref
    base_url = "http://api.crossref.org/works/"
    w_dic = {}
    rels = []
    fout = open('dump_%d.json'%index,'w')
    ferr = open('error_%d.txt'%index,'w')
    fres = open('result_%d.txt'%index,'w')
    diff_dois = {}
    while True:
        line = q.get()
        if line.isdigit(): #serve per svuotare la coda alrimenti la join non ritorna mai
            if int(line) < worker_number:
                q.put(str(int(line)+1))
            fres.write('Numero totale di documenti analizzati %i \n ' %(tot_doc))
            fres.write('Numero di documenti senza riferimenti %i \n' %(no_refs))
            fres.write('Numero di documenti senza DOI fra i riferimenti o con DOI senza informazione %i \n' %(not_responding))
            fres.write('\nTipi di relazioni usate per il riferimento a papers (non necessariamente distinti) \n\n')
            for entry_type in w_dic:#dataset or collection                                                                                                                                     
                fres.write("Relazioni per %s \n" %entry_type)
                for semantic_relation in w_dic[entry_type]:
                    fres.write("Specifica delle relazioni per %s \n \t Tipo del paper riferito e numero di riferimenti trovati \n" % semantic_relation)
                    for referred_entity_type in w_dic[entry_type][semantic_relation]:
                        fres.write("\t %s \t %d \n"%(referred_entity_type,w_dic[entry_type][semantic_relation][referred_entity_type]))
            fres.close()

            fout.close()
            ferr.close()
            fres.close()
            q.task_done()

            print 'Done Worker ' , index
            return

        tot_doc += 1
        data = json.loads(line)
        item = data.keys()[0]
        ref_paper = False
        if data[item][0] == []:
            no_refs += 1
            q.task_done()
            continue
        rels = []
        for k in data[item][0]:
            doi = k[0]["DOI"].strip().encode('ascii','ignore') if  (k[0].keys()[0] == "DOI" and k[0]["DOI"] is not None) else None
                    
            if(doi is None):
                continue
            response_type = ""
            if (doi[:4]=='doi:'):
                doi = doi[4:]
            if (p.match(doi)):
                if doi in diff_dois:
                    response_type = diff_dois[doi]
                else:
                    response_type = responsetype(doi,ferr)
                                           
                diff_dois[doi] = response_type
             
                if response_type == "":
                    continue
                   
                if doi is None:
                    continue
                try:
                    rel_t = w_dic[data[item][1]['resource_type']]
                    try:
                        res_t = rel_t[k[1]['relation_type']]
                        try:
                            res_t[response_type] += 1
                        except:
                            res_t[response_type] = 1
                    except:
                        rel_t[k[1]['relation_type']] = {response_type:1}                                    
                except:
                    w_dic[data[item][1]['resource_type']] = {k[1]['relation_type']:{response_type:1}}
                                
                rels.append([{'DOI':doi,'doc_type':response_type,'relation_type':k[1]['relation_type']}])

        if rels != []:                                          
            result_data = {}
            result_data[item] =[{'element_type':data[item][1]['resource_type']},rels]
            fout.write(json.dumps(result_data) + '\n')
        else:
            not_responding += 1
                
        if ref_paper:
            p_refs += 1
        q.task_done()
                                          
                

q = Queue(maxsize=100)
for i in range(100):
    w = Thread(target=worker,args=(q,i,100,))
    w.daemon = True
    w.start()

count = 0
for line in f:
    count +=1
    q.put(line)
    if count%100==0:
        print "Added %i"%count

q.put('1')
q.join()
f.close()
##f = open('res.txt','w')
##fdump = open('dump.json','w')
##f.write('Numero totale di documenti analizzati %i \n ' %(total_number_of_lines))
##f.write('Numero di documenti senza riferimenti %i \n' %(lines_with_no_references))
##f.write('Numero di documenti senza DOI fra i riferimenti o con DOI senza informazione %i \n' %(lines_with_no_response))
##f.write('\nTipi di relazioni usate per il riferimento a papers (non necessariamente distinti) \n\n')
##for entry_type in references:#dataset or collection
##    f.write("Relazioni per %s \n" %entry_type)
##    for semantic_relation in references[entry_type]:
##        f.write("Specifica delle relazioni per %s \n \t Tipo del paper riferito e numero di riferimenti trovati \n" % semantic_relation)
##        for referred_entity_type in references[entry_type][semantic_relation]:
##            f.write("\t %s \t %d \n"%(referred_entity_type,references[entry_type][semantic_relation][referred_entity_type]))
                                                      



##f.close()
import glob
import os

f = open('dump.json','w')
for fil in glob.glob('dump*.json'):
    if fil == 'dump.json':
        continue
    ff = open(fil)
    for line in ff:
        f.write(line)
    ff.close()
    os.remove(fil)
f.close()

f = open('err.txt','w')
dic = {}
for fil in glob.glob('error_*.txt'):
    ff = open(fil)
    for line in ff:
        try:
            dic[line] 
        except:
            dic[line] = 1
            f.write(line)
    ff.close()
    os.remove(fil)
f.close()

f = open('res.txt','w')
dic = {}
total_number_of_lines = 0
lines_with_no_references = 0
lines_with_no_dois = 0
for fil in glob.glob('result_*.txt'):
    ff = open(fil)
    total_number_of_lines += int(ff.readline().split()[5])
    lines_with_no_references += int(ff.readline().split()[5])
    lines_with_no_dois += int(ff.readline().split()[13])
    for i in range(3):
        ff.readline()
    found = False
    for line in ff:
        if found:
            found = False
            continue
        if 'Relazioni per' in line:
            tipo = line.split()[2]
            continue
        if 'Specifica delle relazioni per' in line:
            relation = line.split()[4]
            found = True
            continue
        line = line.split()
        try:
            rel = dic[tipo]
        
            try:
                pub = rel[relation]
               
                try:
                    pub[line[0]] += int(line[1])
                except:
                    pub[line[0]] = int(line[1])
            except:
                rel[relation] = {line[0]:int(line[1])}
        except:
            try:
                dic[tipo]={relation:{line[0]:int(line[1])}}
            except:
                print line
    ff.close()
    os.remove(fil)

    
f.write('Numero totale di documenti analizzati %i \n ' %(total_number_of_lines))                                                                                                      
f.write('Numero di documenti senza riferimenti %i \n' %(lines_with_no_references))                                                                                                    
f.write('Numero di documenti senza DOI fra i riferimenti o con DOI senza informazione %i \n' %(lines_with_no_dois))                                                               
f.write('\nTipi di relazioni usate per il riferimento a papers (non necessariamente distinti) \n\n')                                                                                  
for entry_type in dic:#dataset or collection                                                                                                                                   
    f.write("Relazioni per %s \n" %entry_type)                                                                                                                                        
    for semantic_relation in dic[entry_type]:                                                                                                                                  
        f.write("Specifica delle relazioni per %s \n \t Tipo del paper riferito e numero di riferimenti trovati \n" % semantic_relation)                                              
        for referred_entity_type in dic[entry_type][semantic_relation]:                                                                                                        
            f.write("\t %s \t %d \n"%(referred_entity_type,dic[entry_type][semantic_relation][referred_entity_type]))                                                          

