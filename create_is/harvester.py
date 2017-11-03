#harvest data from oai-pmh endpoint
from lxml import etree
from StringIO import StringIO
import urllib2

import time

class Harvester():
    def __init__(self, next_cursor = '',prefix = 'datacite', from_date = '', until_date = '', base_url='https://oai.datacite.org/oai?verb=ListRecords'):
        self.next_cursor = next_cursor
        self.prefix = prefix
        self.from_date = from_date
        self.until_date = until_date
        self.base_url = base_url

    def next(self, max_try=10, res_token_query="//*[local-name()='resumptionToken']"):
        current = 0
        url = self.base_url
        if self.next_cursor != '':
            url += "&resumptionToken=" + self.next_cursor
        else:
            url += '&metadataPrefix=%s' % self.prefix
            if self.from_date != '':
                url += '&from=%s' % self.from_date
        print url

        while current < max_try:
            try:
                resp = urllib2.urlopen(url)
                data = resp.read()
                root = etree.parse(StringIO(data))
                rt = root.xpath(res_token_query)
                if rt is None:
                    self.next_cursor = ''
                else:
                    self.next_cursor = rt[0].text
                    print self.next_cursor
                return data
            except Exception as e:
                print "An error occur on download from %s" % url
                time.sleep(5)
                print e
                current += 1
        return None


    def has_next(self):
        return self.next_cursor != ''




