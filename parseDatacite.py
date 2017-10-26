import json
from StringIO import StringIO
from lxml import etree
import re
import sys
#import xml.etree.ElementTree as ET
#associates to each element its descriptions

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
    return ret.lower()

fin = open('datacite.json')
fout = open('DOI-dex-title.txt','w')

print 'associating each element to its description and title ...'
count = 0
for line in fin:
    count += 1
    if count % 1000 == 0:
        print 'analizzati ' + str(count)
    root = parseline(line)
    doi = getidentifier(root)
    if doi == '':
        continue
    result = root.xpath("//*[local-name()='description']")
    input_str = ''
    if len(result)>0:#esiste una descrizione per la risorsa
        abs = ''
        for description in result:
            description_type = description.get('descriptionType')
            if description_type.lower() == 'abstract' and not description.text is None:
                abs += description.text.replace('\n',' ').replace('\r',' ').replace('\t',' ') + '@@@@@@'
                                                                                                                                
        input_str = abs.encode('utf-8','ignore')
    result = root.xpath("//*[local-name()='title']")
    title = ''
    for t in result:
        title += t.text.encode('utf-8','ignore') + ' '

    try:
        #fout.write(doi + '\t$|$\t' + input_str.getvalue() + '\t$|$\t' + title + '\n')
        if input_str == '' and title == '':
            continue
        fout.write(doi.encode('utf-8') + '\t$|$\t' + input_str + '\t$|$\t' + title + '\n')
        
    except:
        print line
        print doi.decode('utf-8')
        print input_str
        print title
        print line
        sys.exit()
fin.close()
fout.close()
