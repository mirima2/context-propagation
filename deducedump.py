import json
import re

f = open('OUTPUT/DUMPS/dump_abs.json')

fout = open('checklines_abs.json','w')

dumpdic = {}
jsondic = {}

for line in f:
    data = json.loads(line)
    for item in data:
        dumpdic[item] = data[item][0]['element_type']

f.close()

f = open('abstractout_v2.json')
for line in f:
    data = json.loads(line)
    for item in data:
        try:
            dumpdic[item]
        except:
            jsondic[item] = data[item][1]['resource_type']
            if data[item][0] == []:
                continue
            for k in data[item][0]:
                if k[0].keys()[0]=='DOI':
                    fout.write(line)
                    break

f.close()
fout.close()
for key in dumpdic:
    jsondic[key] = dumpdic[key]

fout = open('OUTPUT/DUMPS/dump2_abs.json','w')
f = open('checklines_abs.json')
relations = {}
for line in f:
    data = json.loads(line)
    rels = []
    for item in data:
        for k in data[item][0]:
            doi = k[0]["DOI"].strip().encode('ascii','ignore') if  (k[0].keys()[0] == "DOI" and k[0]["DOI"] is not None) else None
            if(doi is None):
                continue
            if (doi[:4]=='doi:'):
                doi = doi[4:]
            if doi in jsondic:
                response_type = jsondic[doi]
                relation_type = k[1]['relation_type']
                try:
                    rel_t = relations[data[item][1]['resource_type']]
                    try:
                        res_t = rel_t[relation_type]
                        try:
                            res_t[response_type] += 1
                        except:
                            res_t[response_type] = 1
                    except:
                        rel_t[relation_type] = {response_type:1}
                except:
                    relations[data[item][1]['resource_type']] = {relation_type:{response_type:1}}


                rels.append([{'DOI':doi,'doc_type':response_type,'relation_type':relation_type}])

    if rels != []:
        result_data = {}
        result_data[item] =[{'element_type':data[item][1]['resource_type']},rels]
        fout.write(json.dumps(result_data) + '\n')
f.close()
fout.close()

fres = open('OUTPUT/RES/sem_info2_abs.txt','w')
fres.write('\nTipi di relazioni usate per il riferimento a papers (non necessariamente distinti) \n\n')
for entry_type in relations:#dataset or collection                                                                                                                 
    fres.write("Relazioni per %s \n" %entry_type)
    for semantic_relation in relations[entry_type]:
        fres.write("Specifica delle relazioni per %s \n \t Tipo del paper riferito e numero di riferimenti trovati \n" % semantic_relation)
        for referred_entity_type in relations[entry_type][semantic_relation]:
            fres.write("\t %s \t %d \n"%(referred_entity_type,relations[entry_type][semantic_relation][referred_entity_type]))
fres.close()
