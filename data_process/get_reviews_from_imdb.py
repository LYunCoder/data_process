# -*- coding: utf-8 -*-
import pymysql

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
#    tokens = word_tokenize(review)
#    tokens = [w for w in tokens if w not in stopwords.words('english')]
    return review

def prem(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS imdb")  # 习惯性
    sql = "CREATE TABLE imdb(user_id VARCHAR(20), movie_id VARCHAR(200), rating VARCHAR(8), review_title VARCHAR(500), review TEXT(15000), link VARCHAR(200))"
             
    cursor.execute(sql)  # 根据需要创建一个表格
    
def create_user_review(db):
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS user_review")
    sql = "CREATE TABLE user_review(user_id VARCHAR(20), review MEDIUMTEXT)"
    cursor.execute(sql)
   
# save file per user with their all reviews
def write_review_file(user_id, review):
    user_file = open("C:\\A work\\Research\\workspace\\Maven-wikifier\\wikifier2subgraph\\data\\IMDB\\user_review\\" +user_id+".txt","w", encoding = "UTF-8")
    try:
        user_file.write(review)
        
    except IOError:
        print ("Error: the write of file is failed")
    else:
        print ("the file write is successful")
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
            sql_review = "select review from imdb where user_id = '%s'" % (user_id)
#            print(sql_review)
            cursor.execute(sql_review)
            userid_reviews = cursor.fetchall()
            user_review_insert(db, user_id, userid_reviews)
            count = count + 1
            print('the number of the current item is:' +str(count))
    except:
        print ("Error: unable to fecth user_ids data")
    
    
def user_review_insert(db, user_id, userid_reviews):
    cursor = db.cursor()   
    review = ""    
    try:
        content = []
        for row1 in userid_reviews:
            clean_after_review = clean_review(row1[0])
#            print(clean_after_review);
            review = review + clean_after_review + "\r\n"
        write_review_file(user_id, review)
#        print("the review is: %s" % review)
        insert_user_reviewsql = "insert into user_review(user_id, review) values(%s, %s)" 
#        content.append(user_id)
#        content.append(review)
        content.append((user_id, review))
        cursor.executemany(insert_user_reviewsql, content)
        db.commit()  
        
    except Exception as e:
            db.rollback()
            print(str(e))
    
    
if __name__ == "__main__":  # 起到一个初始化或者调用函数的作用
    db = pymysql.connect("localhost", "root", "302485", "imdb", charset='utf8')
    create_user_review(db)
    reviewdata_read(db)
    db.close()
#def main():
#    review_data_insert()