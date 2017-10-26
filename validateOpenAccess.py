import json
import re
import threading
import urllib2
from multiprocessing.queues import Queue

f = open('out.json')
p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')


class PopupationWorker(threading.Thread):
    def __init__(self, main_queue, f):
        threading.Thread.__init__(self)
        self.queue = main_queue
        self.main_file = f

    def run(self):
        added = 0
        for line in self.main_file:
            data = json.loads(line)
            self.queue.put(data)
            added +=1
            if (added %100 ) ==0:
                print "Added %i"%added
        self.queue.put("DONE")
        print "DONE"


class WorkerDownloader(threading.Thread):
    def __init__(self, main_queue, base_url, id):
        threading.Thread.__init__(self)
        self.queue = main_queue
        self.base_url = base_url
        self.id = id
        self.out_file = open("out_%i.json"%id,"w")

    def run(self):
        while not self.queue.empty():
            data = self.queue.get()

            if data == 'DONE':
                self.queue.put('DONE')
                print "Wroker Done %i"%self.id
                return
            for item in data:
                result_dictionary=[]
                for k in data[item]:
                    type = k.keys()[0]
                    value = k[type]
                    if (value is not None) and p.match(value):
                        doi = value
                        try:
                            response = urllib2.urlopen(base_url+doi, timeout = 1)
                            crossref_response =json.loads(response.read())
                            result_dictionary.append({doi : crossref_response['message']['ISSN']})
                        except:
                            pass
                if len(result_dictionary) > 0:
                    self.out_file.write("{%s:%s}\n"%(item,json.dumps(result_dictionary)))
                    self.out_file.flush()
result_dictionary ={}
base_url = "http://api.crossref.org/works/"
total = 0
to_process = Queue(maxsize=100)

pop_worker = PopupationWorker(to_process, f)
pop_worker.start()
thread_poll = [WorkerDownloader(to_process, base_url, i) for i in range(100)]
for t in thread_poll:
    t.start()
