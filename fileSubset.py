import gzip
import json
from lxml import etree
from StringIO import StringIO
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

with gzip.open('datacite_native_valid_entries.gz','wb') as fout:
    with gzip.open('datacite_native.gz') as f:
        for line in f:

            try:
                root = parseline(line)
            except:
                   continue

            ide = getidentifier(root)
            # print ide
            if ide == "":
                continue

            fout.write(line)
