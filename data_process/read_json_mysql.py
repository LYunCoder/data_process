# -*- coding: utf-8 -*-
import json
import pymysql
import logging
# 读取review数据，并写入数据库
# 导入数据库成功，总共4736897条记录
def prem(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS imdb_entity")  # 习惯性
    sql = "CREATE TABLE imdb_entity(user_id VARCHAR(20), movie_id VARCHAR(200), rating VARCHAR(8), review_title VARCHAR(500), review TEXT(15000), link VARCHAR(200))"
             
    cursor.execute(sql)  # 根据需要创建一个表格


def reviewdata_insert(db):

    imdb_file = 'C:\\Awork\\Research\\experiment\\Datasets\\data.json'
    f = open(imdb_file)
    data = f.read()
    imdb_data = json.loads(data)
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    logging.info('Reading file %s' % imdb_file)
    logging.info('File %s read' % imdb_file)
    count = 0
    for one_item in imdb_data:
        
        try:
            content = []
            user_id = one_item['user']
            movie_id = one_item['movie']
            rating = one_item['rating']
            title = one_item['title']
            review = one_item['review']
            link = one_item['link']
            content.append((user_id, movie_id, rating, title, review, link))
            #print(content)
            insert_sql = "insert into imdb_entity(user_id, movie_id, rating, review_title, review, link) values (%s, %s, %s, %s,%s, %s)"
            #print(insert_sql)
            cursor = db.cursor()
            cursor.executemany(insert_sql, content)
            db.commit()
            count = count+1
            print('the number of the current item is:' +str(count))
        except Exception as e:
            db.rollback()
            print(str(e))
            break


if __name__ == "__main__":  # 起到一个初始化或者调用函数的作用
    db = pymysql.connect("localhost", "root", "302485", "imdb", charset='utf8')
    cursor = db.cursor()
    prem(db)
    reviewdata_insert(db)
    cursor.close()

#def main():
#    review_data_insert()