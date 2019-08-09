# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
import pymysql
import requests

# 读取review数据，并写入数据库
# 导入数据库成功，总共4736897条记录

def clean_review(review):
    """
    Removes punctuations, stopwords and returns an array of words
    """
#    review = review.replace('!', ' ')
#    review = review.replace('?', ' ')
#    review = review.replace(':', ' ')
#    review = review.replace(';', ' ')
#    review = review.replace(',', ' ')
    review = review.replace('\\','')
    review = review.replace('\n','')
    review = review.replace('\r','')
    review = review.replace('\r\n','')
#    tokens = word_tokenize(review)
#    tokens = [w for w in tokens if w not in stopwords.words('english')]
    return review
    
   
# save file per user with their all reviews
def write_review_file(user_id, content):
    user_file = open("C:\\Awork\\Research\\workspace\\Maven-wikifier\\wikifier2subgraph\\data\\IMDB\\user_reviews\\" +user_id+".txt","w", encoding = "UTF-8")
    try:
        user_file.write(content)
    except IOError:
        print ("Error: the write of file is failed")
    else:
        print ("the file write is successful")
        print("The file name is: "+user_id+".txt")
        user_file.close()

def reviewdata_read(db):
    cursor = db.cursor()
    sql = "SELECT DISTINCT user_id FROM imdb"   
    count = 0
    try:
        cursor.execute(sql)
        results = cursor.fetchall() 
        for row in results:  
            user_id = row[0] 
            print('the user_id is: %s' % user_id)
            sql_review = "select movie_id, review_title, review from imdb where user_id = '%s'" % (user_id)
#            print(sql_review)
            cursor.execute(sql_review)
            movieid_reviews = cursor.fetchall()
           # print(movieid_reviews)
            user_review_insert(user_id, movieid_reviews)
            count = count + 1
            print('the number of the current item is:' +str(count))
    except:
        print ("Error: unable to fecth user_ids data")
        
def get_movie_identifier(movie_id):
    
    url = 'https://query.wikidata.org/sparql'   
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
    #print(identifier)
    return identifier
    
    
def user_review_insert(user_id, movieid_reviews):   
    content = "" 
    review_title = ""  
    movie_id = ""
    clean_after_review = ""
    for row1 in movieid_reviews:
        movie_id = row1[0]
        #movie_identifier = get_movie_identifier(movie_id)
        review_title = row1[1]
        clean_after_review = clean_review(row1[2])
#        print(movie_id)
#        print(clean_after_review)
        content = content + movie_id + ": " +review_title + "\r\n" + clean_after_review + "\r\n" + "\r\n"
#        print(content)
    write_review_file(user_id, content)
#        print("the review is: %s" % review)

    
    
if __name__ == "__main__":  # 起到一个初始化或者调用函数的作用
    db = pymysql.connect("localhost", "root", "302485", "imdb", charset='utf8')
    reviewdata_read(db)
    db.close()
#def main():
#    review_data_insert()