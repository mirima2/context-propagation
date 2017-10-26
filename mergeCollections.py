# from mongo_connector import *
#
# def mergeStores(collection_prefix,stores_number, out_collection_name):
#     mongo_db = MongoConnector("146.48.87.96", "DLIDB")
#     for i in range(stores_number):
#         collection_name = collection_prefix + "_w_" + str(i)
#         mongo_db.mergeCollection(collection_name,out_collection_name)
#
# def selectDataciteSubset():
#     mongo_db = MongoConnector("146.48.87.96", "DLIDB")
#     mongo_db.selectDataciteSubset('gatherer')
# #
# # print "merging collections, datacite....\n"
# # mergeStores("dataciteRelations",100,'gatherer')
# #
# # print "Merging collections, documents....\n"
# # mergeStores("resolvedDocuments",100,'resolved_documents')
#
#
# print "Selection datacite subset''' \n"
# selectDataciteSubset()
#
# print "Finished"