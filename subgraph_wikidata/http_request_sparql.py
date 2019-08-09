 # -*- coding: utf-8 -*-

#Get entity identifier from wikidata
#1. Read href value and relevent surfaceName of entities from html file.
#2. Search identifier of entity from wikidata by href value. 

from bs4 import BeautifulSoup
import pandas as pd
import requests
import itertools
import os
import re
import urllib.request
import pymysql 

def get_html_file():
    path = "C:\Work\entity_linking_result\movie_output"                           # 设置路径
    dirs = os.listdir(path)                    # 获取指定路径下的文件
#    movie_count=0
    for i in dirs:
        file_name = os.path.splitext(i) # 循环读取路径下的文件并筛选输出
        if file_name[1] == ".html": # 筛选csv文件
            movie_id=file_name[0].split(".")[0]
           # print(path+"\\"+i)
            paths = path + "\\"+i
            search_database(paths, movie_id)
#            get_href(path+"\\"+i,movie_id)
#            movie_count = movie_count+1
#            print(movie_count)
            
#search mysql based on movie_id, user_id, and review. Then update identifier info into database.
def search_database(path, movie_id):
    file = open(path, 'r', encoding='utf-8')
    pattern_a = re.compile(r'ur\d{7,8}')
#    str1="<br>ur08193822: Still Engaging."
#    mmm = pattern_a.search(str1)
#    print(mmm)
    for line in file:
        m_a = pattern_a.search(line)
        if m_a:
            user_id = m_a.group(0)
            print(user_id)
        pattern_b = re.compile(r'^.*[a-zA-Z0-9]')
#        str2="<br>"
#        mmm = pattern_b.match(str2)
#        print(mmm)
        m_b = pattern_b.match(line)
        if m_b:
            identifiers, entities = get_a_tag(movie_id, user_id, line)
            if entities:
                update_entities(movie_id, user_id, entities, identifiers)
            


#get a review's entities info
def get_a_tag(movie_id, user_id, line):
    soup = BeautifulSoup(line, 'lxml')
    current_entities = 0
    entities = []
    identifiers = []

    for item in soup.find_all(name='a'):
        href = item.attrs['href']
        identifier = get_identifier(href)
        if identifier =='':
            identifier = get_identifier_wididataweb(href)
#            if identifier == '':
#                #Get the newest link based on href and try it again from wikidata
#                new_href = get_new_href(href)
#                identifier = get_identifier_wididataweb(new_href)
        surface_form = item.string
        #cat = item.attrs['cat']
        identifiers.append(identifier)
        entities.append(surface_form)
        current_entities = current_entities+1

    print(identifiers)
    print(entities)
    return identifiers, entities
    
#update database with identifier and surface_name  CONCAT(identifiers,%s) CONCAT(entities, %s)
def update_entities(movie_id, user_id, entities, identifiers):
    db = pymysql.connect("localhost", "root", "302485", "imdb", charset='utf8')
    cursor = db.cursor()
    update_sql = "update imdb_entity set identifiers = CONCAT(identifiers,',',%s), entities = CONCAT(entities,',', %s) where movie_id = %s and user_id = %s and (review like %s or review_title like %s)"
    params = (",".join(identifiers), ",".join(entities), movie_id, user_id, "%"+entities[-1]+"%", "%"+entities[0]+"%")
    print(params)
    try:
       cursor.execute(update_sql, params)
       db.commit()  
       print("update userid: %s and movie_id: %s successfully!!!"%(user_id, movie_id))
    except Exception as e:
        db.rollback() 
        print(str(e))
    finally:
        cursor.close()
        db.close()

def get_href(path,movie_id):
    htmlfile = open(path, 'r', encoding='utf-8')
    htmlhandle = htmlfile.read()
    soup = BeautifulSoup(htmlhandle, 'lxml')
    current_entities = 0
    data = {}
    result = pd.DataFrame(data,index=[0])
    data['surface_form'] = ''
    data['href'] = ''
    data['cat'] = ''
    data['identifier'] = ''
    new = data
    for item in soup.find_all(name='a'):
       # print(item)
        href = item.attrs['href']
        identifier = get_identifier(href)
        surface_form = item.string
        cat = item.attrs['cat']
       # print(cat)
        new['surface_form'] = surface_form
        new['href'] = href
        new['cat'] = cat
        new['identifier'] = identifier
        result = result.append(new,ignore_index=True)
        current_entities = current_entities+1
    print(current_entities)    
    result.to_excel('C:\\Awork\\Research\\workspace\\Maven-wikifier\\wikifier2subgraph\\data\\IMDB\\movie_identifiers\\'+movie_id+'_identifiers'+'.xlsx')
 
def get_identifier(href): 
    if href.split(":")[0]==("http"):
        href = href.split(":")[0]+"s:"+href.split(":")[1]
#    print(href)
    url = 'http://192.168.0.196:9999/bigdata/namespace/wdq/sparql'   
    query = """ 
    prefix schema: <http://schema.org/>
    SELECT * WHERE {
      <%s> schema:about ?item .
    }
    """ %(href)
    r = requests.get(url, params = {'format': 'json', 'query': query})
    data = r.json()
   # print(data)
    identifier = ''
    bindings = data['results']['bindings']
    if bindings:
        identifier = data['results']['bindings'][0]['item']['value'].split('/')[-1]
    #print(identifier)
    return identifier

#Complement of identifiers from official wikidata
def get_identifier_wididataweb(href):
    if href.split(":")[0]==("http"):
        href = href.split(":")[0]+"s:"+href.split(":")[1]
#    print(href)
    url = 'https://query.wikidata.org/sparql'   
    query = """ 
    prefix schema: <http://schema.org/>
    SELECT * WHERE {
      <%s> schema:about ?item .
    }
    """ %(href)
    r = requests.get(url, params = {'format': 'json', 'query': query})
    data = r.json()
   # print(data)
    identifier = ''
    bindings = data['results']['bindings']
    if bindings:
        identifier = data['results']['bindings'][0]['item']['value'].split('/')[-1]
    #print(identifier)
    return identifier

#if cannot get identifier from both local and online wikidata, try to get the content of the webpage then get the identifier

from urllib.parse import quote
import string
def get_new_href(href):
    
#    keyword = href.split('/')[-1]
    #这是代理IP
    proxy = {'http':'117.212.93.131:8080'}
    #创建ProxyHandler
    proxy_support = urllib.request.ProxyHandler(proxy)
    #创建Opener
    opener = urllib.request.build_opener(proxy_support)
    #添加User Angent
    opener.addheaders = [('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36')]
    #安装OPener
    urllib.request.install_opener(opener)
    #使用自己安装好的Opener
    url="https://www.google.dz/search?q=%s"%(href)
    lurl = quote(url, safe=string.printable)
    req = urllib.request.Request(lurl)
    response = urllib.request.urlopen(req).read()
    content = response.decode('utf-8','ignore').replace(u'\xa9', u'')
    pattern1 = re.compile(r'<cite class="iUh30">https://en.wikipedia.org/wiki/([a-zA-Z0-9]+)(_(|\()([a-zA-Z0-9]+)(|\)))*</cite>')
    pattern2 = re.compile(r'<link rel="canonical" href="https://en.wikipedia.org/wiki/([a-zA-Z0-9]+)((|\()(_[a-zA-Z0-9]+)(|\)))*')
    mm = pattern1.search(content)
    m2 = pattern2.search(content)
    new_href = ''
    if mm:
#        print(mm.group())
        new_href = mm.group()[20:-7]
#        print(new_href)
    if m2:
#        print(m2.group())
        new_href = m2.group()
#        print(new_href)
    return new_href
    #link = urllib.request.full_url()
#    print(mm)
#    req = urllib.request.urlopen("https://www.google.dz/search?q=%s"%(href))
#    dlink = req.geturl()
#    print(dlink)
#    keyword = href.split('/')[-1]
#    page = requests.get("https://www.google.dz/search?q=%s"%(href)) 
#    n_url = page.url
#    print(n_url)
#    soup = BeautifulSoup(page.content)
#    links = soup.findAll("a")[0]
#    print(links)

# search wikidat by entity name as entity label        
def search_by_label(entity_name):
    url = 'http://192.168.0.196:9999/bigdata/namespace/wdq/sparql'   
    query = """ 
    PREFIX wdt: <http://www.wikidata.org/prop/direct/>
    SELECT * { ?subject wdt:P345 "%s" }
    """ %(movie_id)
    r = requests.get(url, params = {'format': 'json', 'query': query})
    data = r.json()
    #print(data)
    identifier = ""
    bindings = data['results']['bindings']
    if bindings:
        identifier = bindings[0]['subject']['value'].split('/')[-1]
    else:
        identifier = ""
    
def get_relationship(identifiers):
    
    identifiers = ['Q41148','Q380675','Q22654','Q166389','Q38111','Q44380','Q185079','Q58444','Q206659','Q223110','Q154581']
    #get any two pairs from list
    #lenList = len(identifiers)
    combination = []
    combination = list(itertools.permutations(identifiers,2))   
    #print(combination)
    count = 0
    for com in combination:
        url = 'http://192.168.0.196:9999/bigdata/namespace/wdq/sparql'
        query = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX entity: <http://www.wikidata.org/entity/>
        PREFIX wikibase: <http://wikiba.se/ontology#>
        
        SELECT ?predicate ?predicateLabel WHERE {
          entity:%s ?predicate entity:%s .
          ?property wikibase:directClaim ?predicate .
          ?property rdfs:label ?relationship .
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
        } LIMIT 1 
        """%(com[0],com[1])
        
        #hop is one or two from genre, instance of, subclass of,
        query2 = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX entity: <http://www.wikidata.org/entity/>
        PREFIX wikibase: <http://wikiba.se/ontology#>
        
        """
        
        print(query)
        r = requests.get(url, params = {'format': 'json', 'query': query})
        data = r.json()
        bindings = data['results']['bindings']
        predicate = ""
        if bindings:
            predicate = bindings[0]['predicate']['value'].split('/')[-1]
            count = count+1
        else:
            print("There is no relationship!")
        #print(identifier)
        #print(data)
        print(count)
        print("The entities and their relationship is: %s--%s--%s"%(com[0], predicate, com[1]))
        
def get_path(identifiers):
    query = """
    prefix graph: <http://prism.uvsq.fr/>
    prefix node: <http://prism.uvsq.fr/>
    prefix edge: <http://prism.uvsq.fr#>
    SELECT * FROM graph: WHERE {
      node:a (edge:p|edge:q) ?des.
      ?des (edge:p|edge:q)* node:h.
    }"""

if __name__ == "__main__":  
    
   get_html_file()
    
#   get_new_href('https://en.wikipedia.org/wiki/Inhabited_(group)')
#   get_identifier_content('https://en.wikipedia.org/wiki/The_Obvious_(song)')
    
#    identifiers = ['Q41148','Q380675','Q22654','Q166389','Q38111','Q44380','Q185079','Q58444','Q206659','Q223110','Q154581']
#    get_relationship(identifiers)

    
#import requests
#
#url = 'https://query.wikidata.org/sparql'
#query = """
#SELECT 
#  ?countryLabel ?population ?area ?medianIncome ?age
#WHERE {
#  ?country wdt:P463 wd:Q458.
#  OPTIONAL { ?country wdt:P1082 ?population }
#  OPTIONAL { ?country wdt:P2046 ?area }
#  OPTIONAL { ?country wdt:P3529 ?medianIncome }
#  OPTIONAL { ?country wdt:P571 ?inception. 
#    BIND(year(now()) - year(?inception) AS ?age)
#  }
#  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
#}
#"""
#r = requests.get(url, params = {'format': 'json', 'query': query})
#data = r.json()

