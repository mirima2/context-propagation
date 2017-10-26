from lxml import etree
import urllib2
from StringIO import StringIO

def getroot(response):
    root = None
    try:
        input_str = response.encode('utf-8', 'ignore')
        root = etree.parse(StringIO(input_str))
    except:
        root = etree.parse(StringIO(response))
    return root


def parseArxiv(response):
    root = getroot(response)
    rel = root.xpath("//*[local-name()='feed']/*[local-name()='entry']/*[local-name()='title']")
    titles = []
    for title in rel:
        titles.append(title.text)
    rel = root.xpath("//*[local-name()='feed']/*[local-name()='entry']/*[local-name()='summary']")
    abstracts = []
    for abs in rel:
        abstracts.append(abs.text)

    return titles, abstracts, "publication"

def parsepubmed(response):
    root = getroot(response)
    rel = root.xpath("//ArticleTitle")
    titles = []
    for title in rel:
        titles .append(title.text)
    rel = root.xpath("//Abstract")
    abstracts=[]
    for abs in rel:
        s = ''
        for el in abs:
            if el.attrib != {}:
                if 'NlmCategory' in el.attrib:
                    try:
                        s += el.attrib['NlmCategory'] + ": " + el.text + " "
                    except:
                        s += el.text if el.text is not None else s
            else:
                s += el.text if el.text is not None else s
        abstracts.append(s)
    return titles, abstracts,"publication"

def parseOpenaire(response):
    root = getroot(response)

    title = []
    abs = []
    typology = ''
    titles = root.xpath("//title")
    for t in titles:
        title.append(t.text)
    ab = root.xpath("//description")
    for d in ab:
        abs.append(d.text)

    rtype = root.xpath("//resulttype")

    typology = rtype[0].attrib['classid'] if ((rtype is not None) and (rtype is not [])) else ''

    return title, abs,typology



# res = urllib2.urlopen("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id=2476741")
# print parsepubmed(res.read())

