from mongo_connector import *

mongo_db = MongoConnector("146.48.87.96", "DLIDB")
documents = mongo_db.is_new.find()
prop_collection = mongo_db.toes

def getPropagation(usage):
    tmp =''
    for i in range(3):
        level = usage['level' + str(i +1)]
        if len(level) > 0:
            if tmp == '':
                tmp += '{"usage_value":' + usage['scope'] +','
            else:
                tmp += ','
            tmp += '"level' + str(i + 1) + '":'
        if len(level) == 1:
            tmp += level[0]
        else:
            tmp += "["
            for abs in level:
                tmp = + abs + ","
            tmp = tmp[:-1] + "]"
    return tmp

bulk = open('bulk_prop.json','w')

for document in documents:
    prop_doc = prop_collection.findOne({"id":document["id"]})
    st = '{"index":{"_index":"doidex","_type":"association","_id":"' + document['id'] + '"}}\n'
    st += '{"id":' + document["pid"] + ', "title":' + document["title"][0] + ', "abstract":' + document["abstracts"][0]

    if not prop_doc is None:
        prop = prop_doc['usage']
        first = True

        for usage in prop:

            tmp = getPropagation(usage)
            if not tmp == '':
                st += ','
                if first:
                    first = False
                    st += '"pabs:["'
                st += tmp + '}'
        if first == False:
            st += ']'
    st += '}\n'
    bulk.write(st)
bulk.close()

