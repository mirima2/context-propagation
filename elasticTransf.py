
def createElem(doi,st):
    bulk.write('{"index":{"_index":"doidex","_type":"association","_id":"' + str(p_id) + '"}}\n{"doi":"'+doi+'","dex":"'+st+'"}\n')

bulk = ""
p_id = 0
doi = None
fin = open('DOI-dex.txt')
bulk = open('bulk.json','w')

for line in fin:
    line = line.replace('\n', " ")
#    line = line.replace('\t'," ")
#    line = line.replace('\r'," ")
    line = line.replace('"'," ")
    line = line.replace('\\'," ")
    
    if '\t$|$\t' in line:
        if not doi is None and not st.strip() == "":
            p_id += 1
            createElem(doi,st.replace('\t'," "))
        line = line.split('\t$|$\t')
        doi = line[0]
        st = line[1]
    else:
        st += line + " "
createElem(doi,st)

fin.close()
bulk.close()
