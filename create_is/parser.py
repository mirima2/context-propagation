import re
from utils import *

def parse_datacite(m):

    ides = m.xpath(".//*[local-name()='identifier']")
    pid = ''
    p = re.compile('(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)')
    for ide in ides:
        if ide is None:
            continue
        ide = ide.text.strip().lower()
        ma = p.search(ide)
        if not ma is None:
            pid = ma.group()
            break
    if pid == '':
        return {}

    result = m.xpath(".//*[local-name()='resourceType']")
    try:
        resource = result[0].get('resourceTypeGeneral')
    except:
        #resource = ''
        return {}
    rel_id_result = m.xpath(".//*[local-name()='relatedIdentifier']")
    abss = []
    titles = []
    result = m.xpath(".//*[local-name()='description']")
    for description in result:
        description_type = description.get('descriptionType')
        if description_type == 'Abstract':
            abss.append(description.text)
    result = m.xpath(".//*[local-name()='title']")
    for title in result:
        titles.append(title.text)
    rels = []
    for item in rel_id_result:
        rels.append(
            {"relatedIdentifierType": item.get('relatedIdentifierType'), "relatedIdentifierValue": item.text,
             "relation_type": item.get('relationType').lower()})

    return {'pid': pid, 'title': titles, 'abstracts': abss, 'pid_type': "doi", 'type': resource.lower(),
            'resource_identifier': resource_identifier(pid, "doi"),'rels':rels}


def parseArxiv(root):
    rel = root.xpath("//*[local-name()='feed']/*[local-name()='entry']/*[local-name()='title']")
    titles = []
    for title in rel:
        titles.append(title.text)
    rel = root.xpath("//*[local-name()='feed']/*[local-name()='entry']/*[local-name()='summary']")
    abstracts = []
    for abs in rel:
        abstracts.append(abs.text)

    return titles, abstracts, "publication"

def parsepubmed(root):
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

def parseOpenaire(root):
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
