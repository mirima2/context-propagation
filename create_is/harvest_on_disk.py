from harvester import *
from lxml import etree
from StringIO import StringIO
import gzip
import sys

fout = gzip.open('/data/miriam/context-propagation/datacite_native_new.gz','a')

def import_page(page):
    root = etree.parse(StringIO(page))
    meta = root.xpath("//*[local-name()='metadata']")
    for m in meta:
        fout.write(etree.tostring(m) + '\n')


def save_datacite(from_date=''):
    harvester = Harvester(from_date=from_date)
    print "harvesting from date %s"%from_date
    i=0
    while True:
        page = harvester.next()
        i+=1
        if i%1000==0:
            print "saved %i pages"%i
        while page is None: #page is None because of error in connections with remote endpoint
            harvester.next() #asks for the same page. No change is done on resumption_token
        import_page(page)
        if not harvester.has_next():
            break
    fout.close()

save_datacite(from_date=sys.argv[1])