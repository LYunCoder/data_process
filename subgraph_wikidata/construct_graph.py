# -*- coding: utf-8 -*-
import pymysql
import itertools
import requests

  
#search all of the entities from db and remove duplicated entries.
def pre_deal():
    db = pymysql.connect("localhost", "root", "302485", "imdb", charset='utf8')
    cursor = db.cursor()
    search_sql = """select identifiers from imdb_entity"""
    identifiers = ''
    id_groups = []
    try:
        cursor.execute(search_sql)   
        identifiers = cursor.fetchall()
        identify_groups = []
        for identify in identifiers:            
            split_id = identify[0].split(",")
            identify_groups.extend(split_id)
        identify_groups.sort()
        id_distincts = itertools.groupby(identify_groups)
        for k,g in id_distincts:
            # Store group iterator as a list
            id_groups.append(k)
    except Exception as e:
        db.rollback() 
        print(str(e))
    finally:
        cursor.close()
        db.close()
    return id_groups
        
#search relationships between identifiers
def save_triples():
    identifier_file = 'C:\\Users\\imyun\\Desktop\\all_identifiers.txt'
    f = open(identifier_file)
    data = f.read()
    identifiers = data.split(', ')
    count = len(identifiers)
    print("the number of all identifiers is: %s"%str(count))
    triples = []
    relation_count = 0
    for i in range(0, count):
        for j in range(i + 1, count):
            predicate_one = get_relation(identifiers[i], identifiers[j])
            predicate_two = get_relation(identifiers[j], identifiers[i])
            if(predicate_one!=''):
                triple_one = identifiers[i] + "	" + predicate_one + "	" + identifiers[j]                              
                triples.append(triple_one)
                relation_count = relation_count+1
                print("There are %s relations~~~"%str(relation_count))
                print(triple_one)
            if(predicate_two!=''):
                triple_two = identifiers[j] + "	" + predicate_two + "	" + identifiers[i]
                triples.append(triple_two)
                relation_count = relation_count+1
                print("There are %s relations~~~"%str(relation_count))
                print(triple_two)
    # write into txt file line by line
    file_write_obj = open("C:\Work\IMDB process\IMDb\data\triples.txt", 'w')
    for triple in triples:
        file_write_obj.writelines(triple)
        print(triple)
        file_write_obj.write('\n')
    file_write_obj.close()
    
    return triples
    

def get_relation(identfier_one, identifier_two):
    url = 'http://192.168.0.196:9999/bigdata/namespace/wdq/sparql'
    query = """
    SELECT ?item_one ?predicate ?item_two
    WHERE
    {
      ?item_one ?predicate ?item_two.
      BIND(wd:%s AS ?item_one).
      BIND(wd:%s AS ?item_two).
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
    }
    """ %(identfier_one,identifier_two)

    r = requests.get(url, params = {'format': 'json', 'query': query})
    data = r.json()
    predicate = ''
    bindings = data['results']['bindings']
    if bindings:
        predicate = data['results']['bindings'][0]['predicate']['value'].split('/')[-1]
    return predicate

def search_movies():
    db = pymysql.connect("localhost", "root", "302485", "imdb", charset='utf8')
    cursor = db.cursor()
    search_msql = """select distinct(movie_id) from imdb_entity"""
    try:
       cursor.execute(search_msql)
       movies = cursor.fetchall()  
    except Exception as e:
        db.rollback() 
        print(str(e))
    finally:
        cursor.close()
        db.close()
    
    return movies

#add all movie info into graph based on its ID to get identifier and select response information of it like genre, director.
def add_movie_identifier(movie_ids):
    corr_idents = []
    for movie_id in movie_ids:
        corr_ident = cor_identifier(movie_id)
        corr_idents.extend(corr_ident)
    corr_idents.sort()
    corr_distincts = itertools.groupby(corr_idents)
    corr_groups=[]
    for k,g in corr_distincts:
            # Store group iterator as a list
            corr_groups.append(k)
    return corr_groups   
        
#IMDB ID:P345, genre:P136, director:P57, cast member:P161, composor:P86, character role:P453

def cor_identifier(movie_id):
    url = 'http://192.168.0.196:9999/bigdata/namespace/wdq/sparql'
    query_identifier = """
    SELECT ?movie_id ?identifier
    WHERE
    {
      ?identifier wdt:P345 ?movie_id.
      BIND('%s' AS ?movie_id).
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
    }
    """%(movie_id)
    r_ident = requests.get(url, params = {'format': 'json', 'query': query_identifier})
    data_ident = r_ident.json()
    movie_ident = ''
    bindings_ident = data_ident['results']['bindings']
    if bindings_ident:
        movie_ident = data_ident['results']['bindings'][0]['identifier']['value'].split('/')[-1]
    movie_genres = []
    movie_director = ''
    movie_cast_mems = []
    movie_characters = []
    movie_composor = ''
    cor_ident=[]
    
    if movie_ident:        
        query_genre = """
        SELECT ?movie ?genre ?genreLabel
        WHERE
        {
          ?movie wdt:P136 ?genre.
          BIND(%s AS ?movie).
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
        }
        """%('wd:'+movie_ident)
        r_genre = requests.get(url, params = {'format': 'json', 'query': query_genre})
        data_genre = r_genre.json()       
        bindings_genre = data_genre['results']['bindings']
        genre_len = len(bindings_genre)
        if genre_len>0:
            for i in range(0, genre_len):
                movie_genre = data_genre['results']['bindings'][i]['genre']['value'].split('/')[-1]
                movie_genres.append(movie_genre)
            
        query_director = """
        SELECT ?movie ?director ?directorLabel
        WHERE
        {
          ?movie wdt:P57 ?director.
          BIND(%s AS ?movie).
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
        }
        """%('wd:'+movie_ident)
        r_director = requests.get(url, params = {'format': 'json', 'query': query_director})
        data_director = r_director.json()     
        bindings_director = data_director['results']['bindings']
        if bindings_director:
            movie_director = data_director['results']['bindings'][0]['director']['value'].split('/')[-1]
        
        query_cast_mem = """
        SELECT ?movie ?cast_mem ?cast_memLabel
        WHERE
        {
          ?movie wdt:P161 ?cast_mem.
          BIND(%s AS ?movie).
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
        }
        """%('wd:'+movie_ident)
        r_cast_mem = requests.get(url, params = {'format': 'json', 'query': query_cast_mem})
        data_cast_mem = r_cast_mem.json()
        
        bindings_cast_mem = data_cast_mem['results']['bindings']
        mem_len = len(bindings_cast_mem)
        if bindings_cast_mem:
            for i_mem in range(0, mem_len):
                movie_cast_mem = data_cast_mem['results']['bindings'][i_mem]['cast_mem']['value'].split('/')[-1]
                movie_cast_mems.append(movie_cast_mem)
        
        query_character = """
        SELECT ?movie ?character ?characterLabel
        WHERE
        {
          ?movie p:P161[pq:P453 ?character].
          BIND(%s AS ?movie).
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
        }
        """%('wd:'+movie_ident)
        r_character = requests.get(url, params = {'format': 'json', 'query': query_character})
        data_character = r_character.json()
        
        bindings_character = data_character['results']['bindings']
        mem_len = len(bindings_character)
        if bindings_character:
            for i_mem in range(0, mem_len):
                movie_character = data_character['results']['bindings'][i_mem]['character']['value'].split('/')[-1]
                movie_characters.append(movie_character)
                
        query_composor = """
        SELECT ?movie ?composor ?composorLabel
        WHERE
        {
          ?movie wdt:P86 ?composor.
          BIND(%s AS ?movie).
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }
        }
        """%('wd:'+movie_ident)
        r_composor = requests.get(url, params = {'format': 'json', 'query': query_composor})
        data_composor = r_composor.json()
        bindings_composor = data_composor['results']['bindings']
        if bindings_composor:
            movie_composor = data_composor['results']['bindings'][0]['composor']['value'].split('/')[-1]
        cor_ident.extend(movie_genres)
        cor_ident.extend(movie_cast_mems)
        cor_ident.append(movie_ident)
        cor_ident.append(movie_director)
        cor_ident.append(movie_composor)
        cor_ident.extend(movie_characters)
    return cor_ident
    
def save_all_identifiers(distinct_all):
    file_write_obj = open("C:\Work\IMDB process\IMDb\data\all_identifiers.txt", 'w')
    for identifier in distinct_all:
        file_write_obj.writelines(identifier)
        file_write_obj.write('\n')
    file_write_obj.close()
    
if __name__ == "__main__": 
    distinct_identi = pre_deal()
    print("Got the identifiers of all the reviews!")
    print("The number of distinct identifiers of all reviews is: %s"%len(distinct_identi))#78365
    movies = search_movies()
    print("Got all the movies from database!!")
    corr_distincts = add_movie_identifier(movies)
    print("Got all the related identifiers of movies!!!")
    distinct_identi.extend(corr_distincts)
    distinct_identi.sort()
    distinct_idents = itertools.groupby(distinct_identi)
    distinct_all = []
    for k,g in distinct_idents:
            # Store group iterator as a list
            if k!='':
                distinct_all.append(k)
    print("combine all the identifiers of reviews!!!!")
    save_all_identifiers(distinct_all)

#    save_triples()

    

# -*- coding: utf-8 -*-

