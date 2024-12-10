import csv
import json
from database import connect_to_database


def read_text():
    with open('./tasks/4/_product_data.text', 'r', encoding='utf-8') as file:
        items = []
        item = {}
        for line in file:
            line = line.strip()
            if line == '=====':
                items.append(item)
                item = {}
                continue
            split = line.split('::')
            key = split[0].strip()
            value = split[1].strip() if len(split) > 1 else None
            item[key.strip()] = value
            item['name'] = item.get('name')
            item['price'] = float(item.get('price', 0))
            item['quantity'] = int(item.get('quantity', 0))
            item['fromCity'] = item.get('fromCity')
            item['isAvailable'] = item.get('isAvailable', 'False') == 'True'
            item['views'] = int(item.get('views', 0))
            item['category'] = item.get('category') if 'category' else None
        return items


def create_table(db):
    cursor = db.cursor()
    cursor.execute("""drop table if exists products""")
    cursor.execute("""
    create table if not exists products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    price REAL NOT NULL CHECK (price >= 0),
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    fromCity TEXT,
    isAvailable BOOLEAN,
    views INTEGER NOT NULL,
    category TEXT NULL,
    update_count INTEGER DEFAULT 0
    )
    """)
    db.commit()


def insert_data(db, data):
    cursor = db.cursor()
    for item in data:
        if item['isAvailable'] in ['True', '1']:
            item['isAvailable'] = '1'
        else:
            item['isAvailable'] = '0'
        cursor.execute("SELECT COUNT(*) FROM products WHERE name = ?", (item['name'],))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
            INSERT INTO products (name, price, quantity, fromCity, isAvailable, views, category)
            VALUES (:name, :price, :quantity, :fromCity, :isAvailable, :views, :category)
            """, item)
    db.commit()


def read_updates():
    with open('./tasks/4/_update_data.json', 'r', encoding='utf-8') as file:
        updates = json.load(file)
    return updates


def apply_update(db, update):
    cursor = db.cursor()
    method = update.get('method')
    param = update.get('param')
    name = update.get('name')

    if method == 'available':
        cursor.execute("""
            UPDATE products
            SET isAvailable = ?, update_count = update_count + 1
            WHERE name = ?
        """, (param, name))

    elif method == 'remove':
        cursor.execute("""
            DELETE FROM products WHERE name = ?
        """, (name,))

    elif method == 'price_percent':
        cursor.execute("SELECT price FROM products WHERE name = ?", (name,))
        current_price = cursor.fetchone()
        if current_price:
            new_price = current_price[0] * (1 + param / 100)
            cursor.execute("""
                UPDATE products
                SET price = ?, update_count = update_count + 1
                WHERE name = ?
            """, (new_price, name))

    elif method == 'quantity_add':
        cursor.execute("SELECT quantity FROM products WHERE name = ?", (name,))
        current_quantity = cursor.fetchone()
        if current_quantity:
            new_quantity = current_quantity[0] + param
            cursor.execute("""
                UPDATE products
                SET quantity = ?, update_count = update_count + 1
                WHERE name = ?
            """, (new_quantity, name))

    elif method == 'quantity_sub':
        cursor.execute("SELECT quantity FROM products WHERE name = ?", (name,))
        current_quantity = cursor.fetchone()
        if current_quantity:
            new_quantity = current_quantity[0] - param
            if new_quantity >= 0:
                cursor.execute("""
                    UPDATE products
                    SET quantity = ?, update_count = update_count + 1
                    WHERE name = ?
                """, (new_quantity, name))

    elif method == 'price_abs':
        if param >= 0:
            cursor.execute("""
                UPDATE products
                SET price = ?, update_count = update_count + 1
                WHERE name = ?
            """, (param, name))

    db.commit()


def process_updates(db, updates):
    for update in updates:
            apply_update(db, update)

def first_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT *
        FROM products
        ORDER BY update_count
        LIMIT 10
    ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items


def second_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT quantity,
        SUM(quantity) as sum_quantity,
        MIN(quantity) as min_quantity,
        MAX(quantity) as max_quantity,
        AVG(quantity) as avg_quantity
        FROM products
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items 


def third_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT category,
        COUNT(*) as count,
        sum(views) as sum_views,
        min(views) as min_views,
        max(views) as max_views,
        avg(views) as avg_views
        FROM products
        GROUP BY category
        ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items 


def fourth_query(db):
    cursor = db.cursor()
    result = cursor.execute('''
        SELECT fromCity,
        COUNT(*) as count
        FROM products
        GROUP BY fromCity
        LIMIT 3
    ''')
    items = []
    for row in result.fetchall():
        items.append(dict(row))
    return items 


def save_items(filename, items):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(items, file, ensure_ascii=False, indent=1)


db = connect_to_database()

create_table(db)

data = read_text()
insert_data(db, data)

updates = read_updates()
process_updates(db, updates)

save_items('./results4/task_4_query_1.json', first_query(db))
save_items('./results4/task_4_query_1.json', second_query(db))
save_items('./results4/task_4_query_1.json', third_query(db))
save_items('./results4/task_4_query_1.json', fourth_query(db))

db.close()
