import hashlib

inverse = {"iscitedby":"cites",
           "cites":"iscitedby",
           "issupplementto":"issupplementedby",
           "issupplementedby":"issupplementto",
           "iscontinuedby":"continues",
           "continues":"iscontinuedby",
           "isnewversionof":"ispreviousversionof",
           "ispreviousversionof":"isnewversionof",
           "ispartof":"haspart",
           "haspart":"ispartof",
           "isreferencedby":"references",
           "references":"isreferencedby",
           "isdocumentedby":"documents",
           "documents":"isdocumentedby",
           "iscompiledby":"compiles",
           "compiles":"iscompiledby",
           "isvariantformof":"isoriginalformof",
           "isoriginalformof":"isvariantformof",
           "isidenticalto":"isidenticalto",
           "hasmetadata":"ismetadatafor",
           "ismetadatafor":"hasmetadata",
           "reviews":"isreviewedby",
           "isreviewedby":"reviews",
           "isderivedfrom":"issourceof",
           "issourceof":"isderivedfrom",
           }

def get_inverse(relation):
    return inverse[relation]

def toRemovePrefix(id,prefix):
    found = False
    if (id.strip()[:len(prefix)] == prefix):
        id = id.strip()[len(prefix)]
        found = True
    return id, found

def toStripId(id):
    found = False
    if (id.strip() != id):
        id = id.strip()
        found = True
    return id, found


def findrelationset(drel, irel):
    if drel == []:
        return irel
    if irel == []:
        return drel

    for r in drel:
        i = isin(r,irel)
        if i>-1:
            del irel[i]
            if irel == []:
                return drel

    for e in irel:
        drel.append(e)

    return drel

def isin(r,rels):
    i = 0
    for i in range(len(rels)):
        rel = rels[i]
        if rel['relation_type'].lower() == r['relation_type'].lower() and rel['relatedIdentifierValue'] == r['relatedIdentifierValue']:
            return i
    return -1

def updateIfDifferent(toupdate,doc,abs="abs"):

    dic = {}

    if toupdate['type'] != doc["type"] and doc["type"] == 'publication':
        dic['type'] = 'publication'

    if toupdate['title'] == [] and doc['title'] != []:
        dic['title'] = doc['title']
    if toupdate['abstracts'] == [] and doc[abs] != []:
        dic['abstracts'] = doc[abs]
    rel = findrelationset(toupdate['rels'], doc['rels'])
    if rel != []:
        dic['rels']=rel
    return dic


def resource_identifier(pid, pid_type):
    m = hashlib.md5()

    r = pid.lower() + "::" + pid_type.lower()
    try:
        m.update(r)
    except:
        m.update(r.encode('utf-8'))

    return m.hexdigest()