import sqlite3
from datetime import datetime
from enum import Enum, auto
from dataclasses import dataclass, fields

class types(Enum):
    NULL    = 'NULL'        # The value is a NULL value.
    INTEGER = 'INTEGER'     # The value is a signed integer, stored in 0, 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
    REAL    = 'REAL'        # The value is a floating point value, stored as an 8-byte IEEE floating point number.
    TEXT    = 'TEXT'        # The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
    BLOB    = 'BLOB'        # The value is a blob of data, stored exactly as it was input.

def get_create(enum_class: type[Enum], table_name: str = None) -> str:
    """
    Genera el SQL CREATE TABLE a partir de una clase Enum con definiciones de PRAGMA.

    Parameters
    ----------
    enum_class : Enum
        La clase Enum que representa la estructura de la tabla.
    table_name : str
        Nombre de la tabla. Si no se proporciona, se toma del nombre de la clase.

    Returns
    -------
    str
        Una cadena SQL con la sentencia CREATE TABLE.
    """
    table_name = table_name or enum_class.__name__
    columns = []

    for member in enum_class:
        field: PRAGMA = member.value
        col_parts = [field.name]
        
        if isinstance(field.type, types):
            col_parts.append(field.type.name)
        else:
            col_parts.append(field.type)
        if field.notnull:
            col_parts.append("NOT NULL")
        if field.unique:
            col_parts.append("UNIQUE")
        if field.pk:
            col_parts.append("PRIMARY KEY")
        if field.dflt_value is not None:
            col_parts.append(f"DEFAULT {repr(field.dflt_value)}")

        columns.append(" ".join(col_parts))

    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns) + "\n);"
    return sql

def get_firm(user: str, extra: str):
    date_now = datetime.now().strftime("%Y-%m-%d / %H:%M")
    firm = f"{user} [{date_now}] {extra}"
    if extra:
        firm = firm + " /" + extra
    return firm

@dataclass
class PRAGMA:
    column: int
    name: str
    type: str
    notnull: bool = False
    dflt_value: any = None
    pk: bool = False
    unique: bool = False

    @classmethod
    def get_from_tuple(cls, field: tuple[any]) -> 'PRAGMA':
        return cls(*field)

    @staticmethod
    def get_form_sql(path_db: str, table: str) -> dict[str, 'PRAGMA']:
        conn = sqlite3.connect(path_db)
        cur = conn.cursor()

        sql = f'''PRAGMA index_list({table});'''
        index_rows = cur.execute(sql).fetchall()
        index_uniques = [index[1] for index in index_rows if index[2]]
        uniques = []
        for index in index_uniques:
            index_data = cur.execute(f"PRAGMA index_info({index})").fetchone()
            uniques.append(index_data[2])
        
        sql = f'''PRAGMA table_info({table});'''
        tbl_data = cur.execute(sql).fetchall()
        conn.close()
        data_dict = dict()
        for field in tbl_data:
            field_obj = PRAGMA.get_from_tuple(field)
            if field[1] in uniques: field_obj.unique = True 
            data_dict[field_obj.name] = field_obj
        
        return data_dict


''' ⚠️ Obtener el create de una tabla
conn = sqlite3.connect('database.db')
cursor = conn.cursor()
table = 'users'

sql = f'
SELECT sql FROM sqlite_master 
WHERE type = "table" AND name = "{table}";
'
data = cursor.execute(sql).fetchone()
conn.close()

for row in data:
    print(row)
'''


class SQL:
    def __init__(self, path_db: str) -> None:
        self.__path_db = path_db

    def select(self, sql: str) -> tuple[list, list]:
        """
        Ejecuta una consulta SELECT y retorna los resultados y los nombres de las columnas.

        Parameters
        ----------
        sql : str
            Consulta SQL a ejecutar (debe ser una sentencia SELECT).

        Returns
        -------
        tuple[list, list]
            Una tupla con dos elementos:
            - lista de tuplas: cada tupla representa una fila de la consulta.
            - lista de strings: nombres de las columnas devueltos por la consulta.
        """
        con = sqlite3.connect(self.__path_db)
        cur = con.cursor()
        data = cur.execute(sql).fetchall()
        headers = [field[0] for field in cur.description]
        con.close()
        return data, headers
    
    def commit(self, sql: str, args=None):
        con = sqlite3.connect(self.__path_db)
        cur = con.cursor()
        if args: cur.execute(sql, args)
        else: cur.execute(sql)
        con.commit()
        con.close()

    def insert(self, table: str, values: dict):
        values['firm'] = get_firm()
        columns = ", ".join(values.keys())
        values = tuple(values.values())
        placeholders = ", ".join("?" for _ in values)
        sql = f'''
        INSERT INTO {table} ({columns}) VALUES ({placeholders});
        '''
        self.commit(sql, values)

    # ⚠️ BUG
    def update(self):
        pass


