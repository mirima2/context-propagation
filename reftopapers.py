#-*- coding: latin-1 -*-

import json

papers_list=['monograph','publication','journal-article','proceedings-article','book-chapter']
f = open('dump.json')
num_collection_referring_papers = 0
num_collection_referring_more_than_one_paper = 0
num_dataset_referring_papers = 0
num_dataset_referring_more_than_one_paper = 0

tot_numb_refs_to_paper_collection = 0
tot_numb_refs_to_paper_dataset = 0
for line in f:
    data = json.loads(line)
    count = 0
    for item in data: #un solo item:il doi dell'entry che ha un collegamento semantico con le entry in questa linea
        element_type = data[item][0]['element_type'] #se l'entry è un DS o una collection
        for ref in data[item][1]:
            if ref[0]['doc_type'] in papers_list:
                    count += 1
        if element_type == 'Dataset':
            tot_numb_refs_to_paper_dataset += count
            if count > 0:
                num_dataset_referring_papers += 1
            if count > 1:
                num_dataset_referring_more_than_one_paper += 1
        else:
            tot_numb_refs_to_paper_collection += count
            if count > 0:
                num_collection_referring_papers += 1
            if count > 1:
                num_collection_referring_more_than_one_paper += 1

print "number of collection referring papers: %d \n "%num_collection_referring_papers
print "number of collection referring more than one paper: %d \n" % num_collection_referring_more_than_one_paper
print "total number of papers referred for collection: %d \n " % tot_numb_refs_to_paper_collection
##print "mean of the number of collection referring more than one paper: %f \n" %(float(tot_numb_refs_to_paper_collection)/float(num_collection_referring_papers))

print "number of dataset referring papers: %d \n "%num_dataset_referring_papers
print "number of dataset referring more than one paper: %d \n" % num_dataset_referring_more_than_one_paper
print "total number of papers referred for dataset: %d \n " % tot_numb_refs_to_paper_dataset
##print "mean of the number of dataset referring more than one paper: %f \n" %(float(tot_numb_refs_to_paper_dataset)/float(num_dataset_referring_papers))
