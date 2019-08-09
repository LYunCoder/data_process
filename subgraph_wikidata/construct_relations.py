# -*- coding: utf-8 -*-
import pymysql
import itertools


if __name__ == "__main__": 
    pre_deal()
    
  
#search all of the entities from db and remove duplicated entries.
def pre_deal():
    db = pymysql.connect("localhost", "root", "302485", "imdb", charset='utf8')
    cursor = db.cursor()
    search_sql = """search identifiers from imdb_entity"""
    try:
       cursor.execute(search_sql)  
    except Exception as e:
        db.rollback() 
        print(str(e))
    finally:
        cursor.close()
        db.close()
    identifiers = cursor.fetchall()
    identify_groups = []
    for identify in identifiers:
        split_identfify = identify.split(",")
        identify_groups.append(split_identfify)
    identify_groups.sort()
    id_distincts = itertools.groupby(identify_groups)
    return id_distincts
        
#search relationships between identifiers
def get_relation(identifiers):
    count = len(identifiers)
    triples = []
    for i in range(0, count):
        for j in range(i + 1, count):
            triple_one = get_triple(identifiers[i], identifiers[j])
            triple_two = get_triple(identifiers[j], identifiers[i])
            if(triple_one!=''):
                triples.append(triple_one)
            if(triple_two!=''):
                triples.append(triple_two)
    return triples
    
def get_triple(identfier_one, identifier_two):
    url = 'http://192.168.0.196:9999/bigdata/namespace/wdq/sparql'
    query = """
    SELECT ?item_one ?predicate ?item_two
    WHERE
    {
      ?item_one ?predicate ?item_two.
      BIND(%s AS ?item_one).
      BIND(%s AS ?item_two).
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
    }
    """ %(identfier_one,identifier_two)
        
    r = requests.get(url, params = {'format': 'json', 'query': query})
    data = r.json()
   # print(data)
    identifier = ''
    bindings = data['results']['bindings']
    if bindings:
        identifier = data['results']['bindings'][0]['item']['value'].split('/')[-1]
    #print(identifier)
    return identifier
        

