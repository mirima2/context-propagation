#-*- coding: latin-1 -*-                                                       

import sys
import json
##import re


##p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')


def updaterelationtype(filename):
    relation_type = {}

    f = open(filename)
    for line in f:
        data = json.loads(line)
        for item in data:
            if data[item][0]==[]:
                continue
            for ref in data[item][0]:
                if ref[0].keys()[0]=='DOI':
                    try:
                        rt = relation_type[data[item][1]['resource_type']]
                        try:
                            rt[ref[1]['relation_type']] += 1
                  
                        except:
                  
                            rt[ref[1]['relation_type']] = 1     
                    except:
                  
                        relation_type[data[item][1]['resource_type']] = {ref[1]['relation_type']:1}
                  

    f.close()
    
    return relation_type

def writeseminfo(filename, relation_type):
    f = open(filename,'w')
    for item in relation_type:
        f.write('Semantic relation for ' + item + '\n')
        for rel in relation_type[item]:
            f.write('\t ' + rel + ' ' + str(relation_type[item][rel]) + '\n')
    
    f.close()



writeseminfo('DOI_abs_seminfo.txt',updaterelationtype('abstractout_v2.json'))
writeseminfo('DOI_noabs_seminfo.txt',updaterelationtype('noabstractout_v2.json'))
