import msgpack
import json
from database import connect_to_database


def read_text():
    with open('./tasks/3/_part_1.text', 'r', encoding='utf-8') as file:
        items = []
        item = {}
        for line in file:
            line = line.strip().lower()
            if line == '=====':
                items.append(item)
                item = {}
                continue
            split = line.split('::')
            key = split[0].strip()
            value = split[1].strip() if len(split) > 1 else None
            item[key.strip()] = value
            item['artist'] = item.get('artist')
            item['song'] = item.get('song')
            item['duration_ms'] = int(item.get('duration_ms', 0))
            item['year'] = int(item.get('year', 0))
            item['tempo'] = float(item.get('tempo', 0))
            item['genre'] = item.get('genre')
            item['instrumentalness'] = float(item.get('instrumentalness', 0))
            item['explicit'] = bool(item.get('explicit', 0))
            item['loudness'] = float(item.get('loudness', 0))
        return items
            

def read_msgpack():
    with open('./tasks/3/_part_2.msgpack', 'rb') as file:
        updates = msgpack.unpack(file)
    for update in updates:
        update['artist'] = str(update['artist']).lower()
        update['song'] = str(update['song']).lower()
        update['duration_ms'] = int(update['duration_ms'])
        update['year'] = int(update['year'])
        update['tempo'] = float(update['tempo'])
        update['genre'] = str(update['genre']).lower()
        update['mode'] = int(update['mode'])
        update['speechiness'] = float(update['speechiness'])
        update['acousticness'] = float(update['acousticness'])
        update['instrumentalness'] = float(update['instrumentalness'])
    return updates


def create_table(db):
    db.execute("""drop table if exists songs;""")
    db.execute("""
    CREATE TABLE IF NOT EXISTS songs
    (id INTEGER PRIMARY KEY,
    artist TEXT,
    song TEXT,
    duration_ms INTEGER,
    year INTEGER,
    tempo FLOAT,
    genre TEXT,
    instrumentalness FLOAT)
    """)


def insert_data(db, data):
    db.executemany('''
    INSERT OR IGNORE INTO songs (artist, song, duration_ms, year, tempo, genre, instrumentalness) 
    VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre, :instrumentalness)''', data)
    db.commit()


def first_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT *
        FROM songs
        WHERE instrumentalness > 0.7
        LIMIT 59
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT
        sum(tempo) as sum_tempo,
        min(tempo) as min_tempo,
        max(tempo) as max_tempo,
        avg(tempo) as avg_tempo
        FROM songs
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT
        COUNT(*) as count,
        artist
        FROM songs
        GROUP BY artist
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT * 
        FROM songs
        WHERE year > 2015
        ORDER BY artist DESC
        LIMIT 64
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def save_items(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


data_text = read_text()
data_msgpack = read_msgpack()

db = connect_to_database()

create_table(db)
insert_data(db, data_text)
insert_data(db, data_msgpack)
db.commit()

save_items('./results3/task_3_query_1.json', first_query(db))
save_items('./results3/task_3_query_2.json', second_query(db))
save_items('./results3/task_3_query_3.json', third_query(db))
save_items('./results3/task_3_query_4.json', fourth_query(db))

db.close()
