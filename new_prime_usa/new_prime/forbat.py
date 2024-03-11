import math,random

import pymysql

def make_bat():
    db_host = "localhost"
    db_user = "root"
    db_password = "actowiz"
    db_name = "new_prime_us"

    main_tbl='us_movies_link'
    # main_tbl='asset_table'
    # scrapy_name='scrapy'
    scrapy_name='scrapy'
    spider_name='image_updated'
    # spider_name='download_assest'

    conn = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_name,autocommit=True)
    cursor = conn.cursor()
    # "712632    1332361  154724"
    selec_q=f'select id from {main_tbl} where status="Done"'
    cursor.execute(selec_q)
    id_lis=[]
    for i in cursor.fetchall():
        id_lis.append(i[0])

    # id_lis=[i for i in range(50)]
    # print(id_lis)
    no_parts = 70

    part_size=len(id_lis)/no_parts
    part_size=math.floor(part_size)-1
    part_list=[]
    flg=False
    count = 0
    for i in range((len(id_lis) + part_size - 1) // part_size ):
        if i!=0:
            start_=id_lis[(i * part_size)+1]
        else:
            start_ = id_lis[i * part_size]
        try:
            end_=id_lis[(i + 1) * part_size]
        except(IndexError):
            end_=id_lis[-1]
            flg=True
        count+=1
        a = f'start "{start_}_{end_}_{count}" {scrapy_name} crawl {spider_name} -a start={start_} -a end={end_}'
        part_list.append(a)
        if flg:
            break

    print(f'taskkill /im {scrapy_name}.exe')
    print('\n'.join(part_list))

if __name__ == '__main__':
    make_bat()