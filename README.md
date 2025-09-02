# MySQLite
Python SQLite3 SubModule with basic functions

![Last commit](https://img.shields.io/github/last-commit/PaulFilms/MySQLite?label=Último%20commit)


## Installation Method

- Last released version

   ```plaintext
   Not available
   ```

- Latest development version

   ```plaintext
   pip install git+https://github.com/PaulFilms/MySQLite.git@main
   ```

## Examples

Basic SELECT
```Python
import mysqlite

path_db = r'path_to_you_sqlite_db.db'

DB = mysqlite.SQL(path_db)

sql = '''SELECT * FROM your_table'''
data = DB.execute(sql)
```

INSERT
```Python
import mysqlite

path_db = r'path_to_you_sqlite_db.db'

DB = mysqlite.SQL(path_db)

new_value = {'id': 'new_id', 'field1': 1234}

DB.insert(table='your_table', values=new_value)
```

UPDATE
```Python
import mysqlite

path_db = r'path_to_you_sqlite_db.db'

DB = mysqlite.SQL(path_db)

new_value = {'id': 'new_id', 'field1': 1234}
where = {'id': 'TEST'}

DB.update(table='your_table', values=new_value, where=where)
```

DATATYPES
```Python
from mysqlite import datatypes

...
```

## Task
   - Translate all text to english
   - Wrtite DATATYPES examples
   - Add Return bool in 'insert' and 'update' functions
   - Get more control with fetchall() / fetchmany(size)
      - fetchall() gets all recordset
      - fetchmany(size) gets size defined recordset


## Extra

Get the CREATE string of a table from a query

```Python
conn = sqlite3.connect(<your db>)
cursor = conn.cursor()
table = 'your table'

sql = f'''
SELECT sql FROM sqlite_master 
WHERE type = "table" AND name = "{table}";
'''

data = cursor.execute(sql).fetchone()
conn.close()

for row in data:
    print(row)
```