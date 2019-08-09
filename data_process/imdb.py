# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 14:50:14 2018

@author: LiuYun
"""
#import json
import logging
from indexer import Indexer
#imdb_file = open('C:\\D\\Research\\experiment\\Datasets\\data.json', encoding='utf-8')
#line = imdb_file.readline()



#while line:
#    print(line)
#    d=json.loads(line)
#    print(d)
#    print(type(d))
#    line = imdb_file.readline()
   
#imdb_file.close()

imdb = Indexer()
imdb_file = 'C:\\D\\Research\\experiment\\Datasets\\data.json'
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.info('Reading file %s' % imdb_file)
imdb.read_file(imdb_file)
logging.info('File %s read' % imdb_file)
(vocab_size, user_list, movie_list, \
rating_matrix, review_matrix, review_map, longest_movie_id, longest_user_id, longest_title, longest_review, longest_link) = imdb.get_mappings()

# Get number of users and movies
Users = len(user_list)
Movies = len(movie_list)

logging.info('No. of users U = %d' % Users)
logging.info('No. of movies M = %d' % Movies)
print(Users) #4079
print(Movies) #4670
print(longest_movie_id) #101
print(longest_user_id) #10
print(longest_title)#207
print(longest_review)#12141
print(longest_link)#137
# The number of reviews is 135669