from harvester import *
from lxml import etree
from StringIO import StringIO
from parser import parse_datacite
from Queue import Queue
from threading import Thread, Lock
from Builder import *
import sys
import glob
import gzip
import json

worker_number = 20
q = Queue(maxsize=worker_number*5)
harvester = Harvester()

def import_page(page):
    root = etree.parse(StringIO(page))
    meta = root.xpath("//*[local-name()='metadata']")
    for m in meta:
        dic = parse_datacite(m)
        if dic == {}:
            continue
        q.put(dic)

def build_is():
    while True:
        page = harvester.next()
        while page is None: #page is None because of error in connections with remote endpoint
            harvester.next() #asks for the same page. No change is done on resumption_token
        import_page(page)
        if not harvester.has_next():
            break

def parseline(line):
    body = json.loads(line)['body']
    input_str = body.encode('utf-8', 'ignore')
    data = StringIO(input_str)
    root = etree.parse(data)
    return root

def build_is_file(path='/data/miriam/context-propagation/datacite_dump'):
    for name in glob.glob(path + '/*.gz'):
        f = gzip.open(name)
        for line in f:
            try:
                root = parseline(line)
            except:
                continue
            dic = parse_datacite(root)
            if dic=={}:
                continue
            q.put(dic)

def run():
    for i in range(worker_number):
        wr = Builder()
        w = Thread(target=wr.run,args=(q,i,worker_number,))
        w.daemon = True
        w.start()

    build_is()
    q.put(1)
    q.join()

    #TODO: put in a single collection all the datacite_document collections
    #TODO: select the subset regarding dataset, collection and publications


def run_test():
    wr = Builder()
    w = Thread(target=wr.run,args=(q,1,1,))
    w.daemon = True
    w.start()
    f = gzip.open('/data/miriam/context-propagation/datacite_dump/datacite_native.gz')
    i = 0
    for i in range(5000):
        line = f.readline()
        try:
            root = parseline(line)
        except:
            continue
        dic = parse_datacite(root)
        if dic == {}:
            continue

        q.put(dic)
    q.put(1)
    q.join()

if __name__ == '__main__':
    if sys.argv[1] == 'TEST':
        run_test()
    else:
        run()