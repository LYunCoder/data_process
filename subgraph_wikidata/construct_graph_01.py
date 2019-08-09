# -*- coding: utf-8 -*-
import requests
import itertools
  
   
def find_relation_everyE():
    identifier_file = 'C:/Users/imyun/Desktop/all_identifiers.txt'
    f = open(identifier_file, encoding="utf-8")
    data = f.read()
    identifiers = data.split(', ')
    count = len(identifiers)
    print("the number of all identifiers is: %s"%str(count))
    triples = []
    relation_count = 0
#    triples = get_relations(identifiers[0])
    for i in range(0, count):
        triple_list = get_relations(identifiers[i])
        triples.extend(triple_list)
    relation_count = len(triples)
    print("the number of all the triples is: %s"%(relation_count))   
    file_write_obj = open("C:/Users/imyun/Desktop/triples.txt", 'w', encoding="utf-8")
    triples.sort()
    triples_distincts = itertools.groupby(triples)
    triple_c = 0
    for k,g in triples_distincts:
        file_write_obj.writelines(k)
        triple_c = triple_c+1
        print(k)
        file_write_obj.write('\n')
    file_write_obj.close()
    print("The number of distinct tripls is: %s"%(triple_c))    
    
def get_relations(identifier):
    url = 'http://192.168.0.196:9999/bigdata/namespace/wdq/sparql'
    query = """
    SELECT ?item_one ?predicate ?item_two
    WHERE
    {
      ?item_one ?predicate ?item_two.
      BIND(wd:%s AS ?item_one).
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
      FILTER(REGEX(STR(?predicate), ".P[0-9]+")) 
      FILTER(REGEX(STR(?item_two), ".Q[0-9]+$"))
    }
    """ %(identifier)
    triples = []
    r = requests.get(url, params = {'format': 'json', 'query': query})
    data = r.json()
#    print(data)
    predicate = ''
    bindings = data['results']['bindings']
    bindings_len = len(bindings)
    print("the bindings length is: %s"%(bindings_len))
    if bindings:
        for i in range(0, bindings_len):
            predicate = data['results']['bindings'][i]['predicate']['value'].split('/')[-1]
    #        print(predicate)
            object_entity = data['results']['bindings'][i]['item_two']['value'].split('/')[-1]
    #        print(object_entity)
            triple = identifier + "	" + predicate + "	" + object_entity
            triples.append(triple)
    return triples
    
if __name__ == "__main__": 

#    get_relations("Q214788")
    find_relation_everyE()
    

# -*- coding: utf-8 -*-


