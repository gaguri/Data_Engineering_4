import csv
import json
from database import connect_to_database

def read_csv():
    items = []
    with open('./tasks/1/item.csv', 'r', encoding='utf-8') as file:
        data = csv.reader(file, delimiter=';')
        data.__next__()
        for row in data:
            if len(row) == 10:
                items.append({
                    'id': int(row[0]),
                    'name': row[1],
                    'street': row[2],
                    'city': row[3],
                    'zipcode': int(row[4]),
                    'floors': int(row[5]),
                    'year': int(row[6]),
                    'parking': row[7].strip().lower() in ['true', 1],
                    'prob_price': int(row[8]),
                    'views': int(row[9])
            })
    return items     
  
def create_table(db):
    db.execute("""drop table if exists house_info""")
    db.execute("""
    create table if not exists house_info (
    id INTEGER PRIMARY KEY,
    name STRING,
    street STRING,
    city STRING,
    zipcode INTEGER,
    floors INTEGER,
    year INTEGER,
    parking BOOLEAN,
    prob_price INTEGER,
    views INTEGER)
    """)
    db.commit()


def insert_data(db, items):
    db.executemany('''
    INSERT OR REPLACE INTO house_info (id, name, street, city, zipcode, floors, year, parking, prob_price, views) 
    VALUES (:id, :name, :street, :city, :zipcode, :floors, :year, :parking, :prob_price, :views)''', items)
    db.commit()


def first_query(db):
    db.cursor()
    result = db.execute('''
        SELECT *
        FROM house_info
        ORDER BY floors
        LIMIT 59
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def second_query(db):
    db.cursor()
    result = db.execute('''
        SELECT
        MIN(floors) as min_floors,
        MAX(floors) as max_floors,
        ROUND(AVG(floors), 0) as avg_floors
        FROM house_info
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT parking,
        COUNT(*) as count
        FROM house_info
        GROUP BY parking
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items 


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT *
        FROM house_info
        WHERE parking = 1
        ORDER BY prob_price DESC
        LIMIT 59
    ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items 


def save_items(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


items = read_csv()

db = connect_to_database()

create_table(db)
insert_data(db, items)
db.commit()


save_items('./results1/task_1_query_1.json', first_query(db))
save_items('./results1/task_1_query_2.json', second_query(db))
save_items('./results1/task_1_query_3.json', third_query(db))
save_items('./results1/task_1_query_4.json', fourth_query(db))

db.close()
