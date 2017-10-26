import json
import re

f = open('doaj.csv')
from glob import glob


doaj_out = open('doaj_out.json', 'w')



issn ={}
eissn = {}

for line in f:
    data = line.strip().split(',')
    if len(data[3].strip()) > 0:
        issn[data[3].strip()] = data[0]
    if len(data[4].strip()) > 0:
        eissn[data[4].strip()] = data[0]

p =re.compile(r'datacite')


def checkIfinDOAJ():
    datacite_id = data.keys()[0]
    for rels in data[datacite_id]:
        doi = rels.keys()[0]
        for issn_rels in rels[doi]:
            if issn_rels in issn or issn_rels in eissn:
                doaj_out.write("%s \n" % json.dumps(data))
                return


for item in glob('out_*.json'):
    k = open(item)
    for line in k:
        d =line.strip()
        x = [m.start() for m in re.finditer(':', d)]

        s = x[2]
        data =json.loads(d[:s].replace('{oai','{"oai')+'":'+d[s+1:])

        checkIfinDOAJ()







