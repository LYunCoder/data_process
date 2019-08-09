# -*- coding: utf-8 -*-
from sklearn.model_selection import train_test_split
 
#Read triples file
file_path = "C:/Work/entity_learning_result/triples.txt"
triples = []
f = open(file_path, encoding="utf-8")
for triple in f:
    triples.append(triple)
train, tv = train_test_split(triples, test_size = 0.2, random_state=0)
file_write_ob1 = open("C:/Work/entity_learning_result/triples_datasets/train.txt", 'w', encoding="utf-8")
for train_tri in train:    
    file_write_ob1.write(train_tri)
file_write_ob1.close()
test, valid = train_test_split(tv, test_size = 0.5, random_state=0)
file_write_ob2 = open("C:/Work/entity_learning_result/triples_datasets/test.txt", 'w', encoding="utf-8")
for test_tri in test:    
    file_write_ob2.write(test_tri)
file_write_ob2.close()
file_write_ob3 = open("C:/Work/entity_learning_result/triples_datasets/valid.txt", 'w', encoding="utf-8")
for valid_tri in valid:    
    file_write_ob3.write(valid_tri)
file_write_ob3.close()