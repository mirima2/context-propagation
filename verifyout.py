import json
import re

edic={}
ferr = open('err.txt')

for line in ferr:
    if 'dli ex' in line:
        continue
    edic[line.split()[2]] = 1

f = open('jsondump.json')
err_dois_count = 0
dois_count = 0
other = 0
no_refs = 0
ok_doi = {}
p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')
for line in f:
    doi = 0
    not_found_doi = 0
    data = json.loads(line)
    item = data.keys()[0]
    if data[item][0] == []:
        no_refs += 1
        continue
    for k in data[item][0]:
            if k[0].keys()[0] == 'DOI':
                 ##   doi += 1
                    d = k[0]["DOI"].strip().encode('ascii','ignore') if k[0]['DOI'] is not None else None
                    if d is None:
                        continue
                    if (d[:4]=='doi:'):
                        d = d[4:]
                    if not p.match(d):
                        continue
                    doi += 1 
                    if d in edic:
                        not_found_doi += 1
                    else:
                        ok_doi[d] = 1
    if doi > 0 and doi == not_found_doi:
            err_dois_count +=1
    elif doi > 0 :
            dois_count += 1
    else:
            other +=1

f.close()
ferr.close()

f = open('dump.json')
for line in f:
    data = json.loads(line)
    item = data.keys()[0]
    for elem in data[item][1]:
        doi = elem[0]['DOI'].encode('ascii','ignore')
        try:
            ok_doi[doi] += 1
        except:
            print 'not in ok_dois %s'%doi

f.close()
print 'not found in dump ' 
ferrdnet = open('dnetnotfoundtype.txt','a')
for key in ok_doi.keys():
    if ok_doi[key] > 1:
        del ok_doi[key]
    else:
        ferrdnet.write("%s \n" %key)
ferrdnet.close()

print len(ok_doi)
dnet_err_doi = 0
if len(ok_doi) > 0:
    f = open('jsondump.json')
    fo = open('dumplines.json','w')
    
    for line in f:
        data = json.loads(line)
        item = data.keys()[0]
        doi=0
        not_found_doi=0
        if data[item][0] == []:
            continue
        for k in data[item][0]:
            if k[0].keys()[0] == 'DOI':
                 ##   doi += 1                                                                                                         
                    d = k[0]["DOI"].strip().encode('ascii','ignore') if k[0]['DOI'] is not None else None
                    if d is None:
                        continue
                    if (d[:4]=='doi:'):
                        d = d[4:]
                    if not p.match(d):
                        continue
                    doi += 1
                    if d in ok_doi:
                        not_found_doi += 1
                   
        if doi > 0 and doi == not_found_doi:
            dnet_err_doi +=1
            fo.write(line)

f.close()
fo.close()


print 'err_dois_count %d \n'%err_dois_count
print 'dois_count %d \n'%dois_count
print 'other %d \n'%other
print 'no_refs %d \n'%no_refs
print 'dnet err dois %d'%dnet_err_doi
