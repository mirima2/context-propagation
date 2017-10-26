import sys
import json
from collections import namedtuple
from StringIO import StringIO
import xml.etree.ElementTree as ET

dic_ds = {} #association doi dataset/collection - doi dataset related to it
dic_pp = {} #association doi dataset/collection - doi publications related to it
dic_n = {} #association between DOI and node number

graph = []
Node = namedtuple("Node","id abs_list abs adj_list publ")


def getNode(key,publ):
    global dic_n
    global graph
    try:
        index = dic_n [key]
        n = graph[index]		
    except:
        n = Node(key,{},"",[],publ)
        index = len(graph)
        dic_n[key] = index		
        graph.append(n)
    return n
	
def updateGraph(dic, publ):
    for key in dic:
        n = getNode(key, False)
        for elem in dic[key]:
            nn = getNode(elem,publ)
            nn.adj_list.append(dic_n[key])
            n.adj_list.append(dic_n[elem])

	
fin = open(sys.argv[1])#reads the dump files dump_noabs.json for example

for line in fin:
    connection = json.loads(line)
    for item in connection:
        info = connection[item]
        rel_list = info[1]
        for el in rel_list:
            if el[0]['doc_type'].lower() == 'dataset':
                try:
                    dic_ds[item].append(el[0]['DOI'])
                except:
                    dic_ds[item] = [el[0]['DOI']]
            elif el[0]['doc_type'] == 'publication':
                try:
                    dic_pp[item].append(el[0]['DOI'])
                except:
                    dic_pp[item] = [el[0]['DOI']]

fin.close()

updateGraph(dic_pp,True)
updateGraph(dic_ds, False)

fin = open(sys.argv[2])

#associates to each element its descriptions
print 'associating each element to its descriptions ...'
count = 0
for line in fin:
    count += 1
    if count % 1000 == 0:
        print 'analizzati ' + str(count)
    line = line.split('\t$|$\t')
    if not line[0] in dic_n:
        continue
    
    node = graph[dic_n[doi]]
    node.abs = line[1]

fin.close()
    		
#propagates context. three steps only. The pairs (abstracts, weight) are associated to the propagation number
print "propagating context..."
for i in range(3):
    print "step " , (i+1)
    for j in range(len(graph)):
        node = graph[j]
        for id in node.adj_list:
            neigh = graph[id]
            if neigh.publ:
                continue
            if node.abs != "":
                    neigh.abs_list[j] = 1
        
            for p_dex in node.abs_list:
                if p_dex in neigh.abs_list:
                    prop_lenght = node.abs_list[p_dex] + 1
                    if prop_lenght < neigh.abs_list[p_dex]:
                        neigh.abs_list[p_dex] = prop_lenght
                
   				
print 'Writing the xml document... '
root = ET.Element('Nodes')
for node in graph:
    add = ET.SubElement(root,'Node')
    add.set('identifierType','DOI')
    add.set('identifier',node.id)
    dex = ET.SubElement(add,'Abstract')
    dex.text = node.abs
    pcontext = [''] * 3
    for context in node.abs_list:
        pcontext[context[1]] += graph[context[0]].abs + ' ' 
    for i in range(3):
        context = pcontext[i]
        if context == '':
            break
        add_context = ET.SubElement(add,'PropagatedContext')
        add_context.set('propagation_level',str(i+1))
        add_context.text = context
		
tree = ET.ElementTree(root)
tree.write(open('propagatexContext.xml','w'),encoding='utf-8')
		
	
	
		
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
		
		
		
		
		
		
		
