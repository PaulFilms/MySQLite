import sqlite3, json
from datetime import datetime
from enum import Enum, auto
from dataclasses import dataclass, fields
from typing import Any, List, Tuple, Dict


class datatypes(Enum):
    '''
    SQLite Datatypes Enumeration

    Typenames
    ---------
    INTEGER :
        INT / INTEGER / TINYINT / SMALLINT / MEDIUMINT / BIGINT / UNSIGNED BIG INT / INT2 / INT8
    TEXT :
        CHARACTER(20) / VARCHAR(255) / VARYING CHARACTER(255) / NCHAR(55) / NATIVE CHARACTER(70) / NVARCHAR(100) / TEXT / CLOB
    REAL :
        REAL / DOUBLE / DOUBLE PRECISION / FLOAT
    NUMERIC :
        NUMERIC / DECIMAL(10,5) / BOOLEAN / DATE / DATETIME
    BLOB :
        BLOB / no datatype specified

    Functions
    ---------
    - get_datetime_from_float
    - get_float_from_datetime
    '''
    # NULL    = 'NULL'        # The value is a NULL value.
    INTEGER = 'INTEGER'     # The value is a signed integer, stored in 0, 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
    TEXT    = 'TEXT'        # The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
    REAL    = 'REAL'        # The value is a floating point value, stored as an 8-byte IEEE floating point number.
    NUMERIC = 'NUMERIC'
    BLOB    = 'BLOB'        # The value is a blob of data, stored exactly as it was input.

    @staticmethod
    def get_datetime_from_float(value: float) -> datetime:
        return datetime.fromtimestamp(value)
    
    @staticmethod
    def get_float_from_datetime(value: datetime) -> float:
        return value.timestamp()


class SQL:
    '''
    SQLite Database Object with basic functions

    Functions
    ---------
    - execute
    - select
    - insert
    - update
    '''
    def __init__(self, path_db: str) -> None:
        self.__path_db = path_db
        self.curr: sqlite3.Cursor = None

    @staticmethod
    def __connection(func):
        def wrapper(self, *args, **kwargs):
            conn = sqlite3.connect(self.__path_db)
            self.curr = conn.cursor()
            commit = kwargs.get('commit', False)
            try: 
                result = func(self, *args, **kwargs)
                if commit: conn.commit()
                return result
            finally:
                conn.close()
        return wrapper
    
    @__connection
    def execute(self, sql: str, values: list[any] | tuple[any] = None, fetch: int = 2, commit: bool = False):
        '''
        Extended sql execute function

        Parameters
        ----------
        sql : str
            SQL string to execute
        
        values :  list[any] or tuple[any]
            Optional iterable object to use in case that sql string use '?' placeholders
        
        Fetch : int
            Integer value with diferent kind of fetch (1.Fecthone, 2.Fecthall, 3.Fecthmany, 4.Description)

        commit : bool
            Boolean value to commit changes in the database

        Returns
        -------
        Fetch :
            - 1. Fecthone -> tuple[any]
            - 2. Fecthall -> list[tuple]
            - 3. Fecthmany -> list[tuple][:size] ⚠️ INCOMPLETE
            - 4. Description -> tuple[str]
        '''
        if values:
            result = self.curr.execute(sql, values)
        else:
            result = self.curr.execute(sql)
        match fetch:
            case 1: return result.fetchone()
            case 2: return result.fetchall()
            case 3: return result.fetchmany()
            case 4: return [field[0] for field in result.description]
    
    def select(self, sql: str) -> list[any]:
        """
        SELECT string function

        Parameters
        ----------
        sql : str
            SQL string to execute

        """
        return self.execute(sql, values=False, fetch=2, commit=False)

    def insert(self, table: str, values: dict[str, any]) -> None:
        """
        INSERT string function

        Parameters
        ----------
        table : str
            String table name
        
        values :  dict[str, any]
            Dictionary with fields and values {'field1': 'New value'}
        """
        # values['firm'] = get_firm('test')
        columns = ", ".join(values.keys())
        values = tuple(values.values())
        placeholders = ", ".join("?" for _ in values)
        sql = f'''INSERT INTO {table} ({columns}) VALUES ({placeholders});'''
        self.execute(sql, values, fetch=0, commit=True)

    def update(self, table: str, values: dict[str, any], where: dict[str, any]) -> None:
        """
        UPDATE string function restricted with mandatory where filter

        Parameters
        ----------
        table : str
            String table name
        
        values :  dict[str, any]
            Dictionary with fields and values {'field1': 'New value'}
        
        where :  dict[str, any]
            Dictionary with where filters {'id': 'the id value'}
        """
        values_str = ', '.join(f"{k}=?" for k, v in values.items())
        where_str = ' AND '.join(f"{k}=?" for k, v in where.items())
        count = self.execute(
            sql=f"SELECT COUNT(*) FROM {table} WHERE {where_str}", 
            values=list(where.values()), 
            fetch=1
        )[0]
        if count:
            self.execute(
                sql=f'''UPDATE {table} SET {values_str} WHERE {where_str};''', 
                values=list(values.values()) + list(where.values()), 
                fetch=0,
                commit=True
            )

    def get_json(self, table: str, json_column: str, where_column: str, where_value: Any) -> Dict[str, Any]:
        '''
        Returns a python dict from defined record

        Parameters
        ----------
        table : str
            String table name
        
        json_column : str
            Column name with json data
        
        where_column : str
            Name of the 'where' filter column
        
        where_value :  str
            Value of the 'where' filter
        '''
        if self.execute(sql=f'SELECT COUNT (*) FROM {table} WHERE {where_column}=?', values=[where_value], fetch=1)[0] != 1:
            return None

        json_data = self.execute(f'SELECT {json_column} FROM {table} WHERE {where_column}=?', values=[where_value], fetch=1)[0]
        if not json_data:
            return None
        
        return json.loads(json_data)
    
    def update_json(self, table: str, json_column: str, where_column: str, where_value: Any, update_values: Dict[str, Any]) -> bool:
        '''
        Updates a json data from defined record

        Parameters
        ----------
        table : str
            String table name
        
        json_column : str
            Column name with json data
        
        where_column : str
            Name of the 'where' filter column
        
        where_value :  str
            Value of the 'where' filter
        
        update_values : dict[str, any]
            New values to add or update
        '''
        if self.execute(sql=f'SELECT COUNT (*) FROM {table} WHERE {where_column}=?', values=[where_value], fetch=1)[0] != 1:
            return False

        json_dict = self.get_json(table=table, json_column=json_column, where_column=where_column, where_value=where_value)
        
        if not json_dict:
            json_dict = update_values
        else:
            for k, v in update_values.items():
                json_dict[k] = v
        
        self.update(table=table, values={json_column: json.dumps(json_dict)}, where={where_column: where_value})
        return True


class SCHEMA:

    @dataclass
    class FIELD:
        '''
        Table Schema Field

        Parameters
        ----------
            column : int
            name : str
            type : str
            notnull : bool = False
            dflt_value : any = None
            pk : bool = False
            unique : bool = False
        '''
        column: int
        name: str
        type: str
        notnull: bool = False
        dflt_value: any = None
        pk: bool = False
        unique: bool = False

        @classmethod
        def get_from_tuple(cls, field: tuple[any]) -> 'SCHEMA.FIELD':
            '''
            Returns a SCHEMA.FIELD object from PRAGMA tuple data

            Parameters
            ----------
            field: tuple[any]
                Tuple with 
            '''
            return cls(*field)

        @staticmethod
        def get_table_fields(path_db: str, table: str) -> dict[str, 'SCHEMA.FIELD']:
            '''
            Returns a dict with values SCHEMA.FIELD of the defined table

            Parameters
            ----------
            path_db : str
                Path string of SQLite Database
            table : str
                Tuple with the values returned by the 'PRAGMA table_info' sql
            '''
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
                field_obj = SCHEMA.FIELD.get_from_tuple(field)
                if field[1] in uniques: field_obj.unique = True 
                data_dict[field_obj.name] = field_obj
            
            return data_dict


    def get_sql_create(enum_class: type[Enum], table_name: str = None) -> str:
        """
        Gets a CREATE TABLE string from Enum class with SCHEMA.FIELD definitions.

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
        
        Example
        -------
        ````
        class users(Enum):
            id = SCHEMA.FIELD(column=0, name='id', type='INTEGER', notnull=True, dflt_value=None, pk=True, unique=True)
            name = SCHEMA.FIELD(column=1, name='name', type='TEXT', notnull=True, dflt_value=None, pk=False)
            last_name = SCHEMA.FIELD(column=2, name='last_name', type='TEXT', notnull=True, dflt_value=None, pk=False)

        ```
        """
        table_name = table_name or enum_class.__name__
        columns = []

        for member in enum_class:
            field: SCHEMA.FIELD = member.value
            col_parts = [field.name]
            
            if isinstance(field.type, datatypes):
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
