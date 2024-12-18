import pandas as pd
import json
import pickle
from database import connect_to_database


def create_wine_table(db):
    with open('./tasks/5/wine.pkl', 'rb') as file:
        data = pickle.load(file)
        data = data.rename(columns={'id': 'wine_id', 'title': 'overview'})
        wine = data[['wine_id', 'description', 'points', 'price', 'taster_name', 'overview']].to_dict(orient='records')
        cursor = db.cursor()
        cursor.execute('''drop table if exists wine''')
        cursor.execute('''
        create table if not exists wine (
        wine_id integer primary key,
        description text,
        points integer,
        price real,
        taster_name text,
        overview text)
        ''')
        cursor.executemany('''
        insert into wine (wine_id, description, points, price, taster_name, overview)
        values (:wine_id, :description, :points, :price, :taster_name, :overview)
        ''', wine)
        db.commit()
        return wine
    

def create_winery_table(db):
    with open('./tasks/5/winery.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
        winery = []
        for row in data:
            if len(row) == 0: continue
            winery.append({
                'wine_id': int(row['id']),
                'winery': row['winery'],
                'designation': row['designation'],
                'variety': row['variety'],
            })
        cursor = db.cursor()
        cursor.execute('''drop table if exists winery''')
        cursor.execute('''
        create table if not exists winery (
        wine_id integer references wine(wine_id),
        winery text,
        designation text,
        variety text)
        ''')
        cursor.executemany('''
        insert into winery (wine_id, winery, designation, variety)
        values (:wine_id, :winery, :designation, :variety)
        ''', winery)
        db.commit()
        return winery


def create_region_table(db):
        data = pd.read_csv('./tasks/5/region.csv', encoding='utf-8')
        data = data.rename(columns={'id': 'wine_id', 'region_1': 'region', 'region_2': 'district'})
        region = data[['wine_id', 'winery', 'country', 'province', 'region', 'district']].to_dict(orient='records')
        cursor = db.cursor()
        cursor.execute('''drop table if exists region''')
        cursor.execute('''
        create table if not exists region (
        wine_id integer references wine(wine_id),
        winery text references winery(winery),
        country text,
        province text,
        region text,
        district text)
        ''')
        cursor.executemany('''
        insert into region (wine_id, winery, country, province, region, district)
        values (:wine_id, :winery, :country, :province, :region, :district)
        ''', region)
        db.commit()
        return region


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
    select wine.wine_id, winery.variety, wine.price, wine.points
    from wine
    join winery on wine.wine_id = winery.wine_id
    where points > 90
    group by winery.variety
    order by wine.price desc
    limit 50
    ''')
    query = []
    for row in result.fetchall():
        query.append(dict(row))
    return query


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
    select count(distinct winery.designation) as num_designation
    from winery
    join region on winery.winery = region.winery
    where country != 'France'
    ''')
    query = []
    for row in result.fetchall():
        query.append(dict(row))
    return query


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
    select
    min(wine.price) as min_price,
    max(wine.price) as max_price,
    avg(wine.price) as avg_price,
    region.country
    from wine
    join region on wine.wine_id = region.wine_id
    where country = 'US'
    ''')
    query = []
    for row in result.fetchall():
        query.append(dict(row))
    return query


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
    select
    province,
    region,
    count (distinct winery) as wineries
    from region
    where province = 'Sicily & Sardinia'
    group by region
    ''')
    query = []
    for row in result.fetchall():
        query.append(dict(row))
    return query


def fifth_query(db):
    cursor = db.cursor()
    cursor.execute('''
    delete from region
    where district is null
    ''')
    db.commit()

    rows_deleted = cursor.rowcount
    print(f"Удалено {rows_deleted} строк, в которых отсутствует поле 'disctrict'.")
    return rows_deleted


def sixth_query(db):
    cursor = db.cursor()
    cursor.execute('''
    update wine
    set price = 30
    where price is null
    ''')
    db.commit()

    updated_rows = cursor.rowcount
    print(f"Количество строк с обновленной ценой: {updated_rows}")
    return updated_rows


def save_queries(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


db = connect_to_database()
wine = create_wine_table(db)
winery = create_winery_table(db)
region = create_region_table(db)

save_queries('./results5/task_5_query_1.json', first_query(db))
save_queries('./results5/task_5_query_2.json', second_query(db))
save_queries('./results5/task_5_query_3.json', third_query(db))
save_queries('./results5/task_5_query_4.json', fourth_query(db))
fifth_query(db)
sixth_query(db)