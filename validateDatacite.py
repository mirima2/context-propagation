from StringIO import StringIO
import sys
from lxml import etree
import json
reload(sys)
sys.setdefaultencoding('utf-8')
f = open('datacite.json')
rfile = open('out.json','w')

parsed = 0


for line in f:
    body = json.loads(line)['body']
    input_str =body.encode('utf-8', 'ignore')
    data = StringIO(input_str)
    root = etree.parse(data)
    result = root.xpath("//*[local-name()='relatedIdentifier']")
    if len(result) > 0:
        result_data = { }
        rels = []
        for item in result:
            rels.append({item.get('relatedIdentifierType'):item.text})
        result = root.xpath("//*[local-name()='recordIdentifier']")[0].text
        result_data[result] =rels
        rfile.write(json.dumps(result_data))
        rfile.write('\n')
    parsed += 1

    if (parsed % 10000) == 0:
        print "Parsed %i"%parsed
        rfile.flush()

rfile.close()
