import pickle
import json
from database import connect_to_database

def read_pickle():
    with open('./tasks/2/subitem.pkl', 'rb') as file:
        data = pickle.load(file)
    return data


def create_table(db):
    db.execute("""drop table if exists house_stats;""")
    db.execute('''
        create table if not exists house_stats (
        id INTEGER PRIMARY KEY,
        name TEXT references house_info(name),
        rating FLOAT,
        convenience INTEGER,
        security INTEGER,
        functionality INTEGER,
        comment TEXT)
        ''')


def insert_data(db, items):
    db.executemany('''
    INSERT OR IGNORE INTO house_stats (name, rating, convenience, security, functionality, comment) 
    VALUES (:name, :rating, :convenience, :security, :functionality, :comment)''', items)
    db.commit()

def first_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT *
        FROM house_stats
        WHERE name = 'Пандус 16'
        ORDER BY convenience DESC
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT h.name, h.city, s.security, s.functionality
        FROM house_info h
        JOIN house_stats s ON h.name = s.name
        WHERE floors >= 4
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT h.name, max(s.rating) as max_rating, count(s.comment) as comments
        FROM house_info h
        JOIN house_stats s ON h.name = s.name
        GROUP BY h.name 
        ORDER BY s.rating DESC
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def save_items(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


items = read_pickle()

db = connect_to_database()

create_table(db)
insert_data(db, items)
db.commit()

save_items('./results2/task_2_query_1.json', first_query(db))
save_items('./results2/task_2_query_2.json', second_query(db))
save_items('./results2/task_2_query_3.json', third_query(db))

db.close()
