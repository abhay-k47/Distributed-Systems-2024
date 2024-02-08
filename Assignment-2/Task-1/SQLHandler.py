import mysql.connector


class SQLHandler:
    def __init__(self, host='localhost', user='root', password='abc'):
        self.host = host
        self.user = user
        self.password = password

    def connect(self):
        connected = False
        while not connected:
            try:
                self.mydb = mysql.connector.connect(
                    host=self.host, user=self.user, password=self.password)
                connected = True
            except Exception:
                pass

    def query(self, sql, value=None):
        if not hasattr(self, 'mydb') or self.mydb is None:
            self.connect()
        cursor = self.mydb.cursor()
        if value is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, value)
        res = cursor.fetchall()
        cursor.close()
        self.mydb.commit()
        return res

    def querymany(self, sql, values):
        if not hasattr(self, 'mydb') or self.mydb is None:
            self.connect()
        cursor = self.mydb.cursor()
        cursor.executemany(sql, values)
        res = cursor.fetchall()
        cursor.close()
        self.mydb.commit()
        return res

    def hasDB(self, dbname):
        res = self.query("SHOW DATABASES")
        return dbname in [r[0] for r in res]

    def UseDB(self, dbname):
        if not self.hasDB(dbname):
            self.query(f"CREATE DATABASE {dbname}")
        self.query(f"USE {dbname}")

    def DropDB(self, dbname):
        res = self.query("SHOW DATABASES")
        if dbname in [r[0] for r in res]:
            self.query(f"DROP DATABASE {dbname}")

    def CreateTable(self, tabname, columns, dtypes, prikeys):
        res = self.query("SHOW TABLES")
        if tabname not in [r[0] for r in res]:
            dmap = {'number': 'INT', 'string': 'VARCHAR(255)', 'float': 'FLOAT', 'double': 'DOUBLE',
                    'date': 'DATE', 'datetime': 'DATETIME', 'boolean': 'BOOLEAN', 'char': 'CHAR(1)', }
            col_config = ''
            for col, dtype in zip(columns, dtypes):
                constraint = 'PRIMARY KEY' if col in prikeys else ''
                col_config += f"{col} {dmap[dtype.lower()]} {constraint}, "
            col_config = col_config.rstrip(', ')
            self.query(f"CREATE TABLE {tabname} ({col_config})")

    def Exists(self, table_name, col, val):
        res = self.query(f"SELECT * FROM {table_name} where {col}={val}")
        return len(res) > 0

    def Select(self, table_name, col=None, low=None, high=None):
        schema = self.query(f"DESCRIBE {table_name}")
        schema = [col[0] for col in schema]
        if col == None:
            rows = self.query(f"SELECT * FROM {table_name}")
        elif high == None:
            rows = self.query(f"SELECT * FROM {table_name} where {col}={low}")
        else:
            rows = self.query(
                f"SELECT * FROM {table_name} where {col} BETWEEN {low} AND {high}")

        res = [dict(zip(schema, row)) for row in rows]
        return res

    def Update(self, table_name, col, val, data):
        format = ', '.join([f'{col}=%s' for col, val in data.items()])
        values = tuple(data.values())
        self.query(
            f"UPDATE {table_name} SET {format} WHERE {col}={val}", values)

    def Insert(self, table_name, rows):
        schema = self.query(f"DESCRIBE {table_name}")
        schema = [col[0] for col in schema]

        if not all(set(row.keys()) == set(schema) for row in rows):
            raise mysql.connector.Error(
                msg="Invalid row: Does not match table schema")

        format = ", ".join(["%s"]*len(schema))
        schema = ", ".join(schema)
        values = [tuple(row.values()) for row in rows]
        self.querymany(
            f"INSERT INTO {table_name} ({schema}) VALUES ({format})", values)

    def Delete(self, table_name, col, val):
        self.query(f"DELETE FROM {table_name} WHERE {col}={val}")
