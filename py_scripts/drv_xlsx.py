from os import path, getcwd, makedirs
from shutil import move

import pandas as pd

from .dbconnect import Connection
from .driver import Driver
from .environment import FILES_SOURCE_DIR, OUTPUT, SQL_SCRIPTS, FILES_BACKUP_DIR


class XLSX(Driver):
    def __init__(self, table_name, mode, mask, output, src_columns=None, stg_schema=None, stg_columns=None,
                 trg_schema=None, trg_columns=None, source=None, **kwargs):
        super().__init__(mode, table_name, src_columns, stg_columns, trg_columns, stg_schema, trg_schema)
        self.table_name = table_name.upper()
        self.mode = mode
        self.mask = mask
        self.output = output
        self.src_columns = src_columns
        self.stg_schema = stg_schema.upper()
        self.stg_columns = stg_columns
        self.trg_schema = trg_schema.upper()
        self.trg_columns = trg_columns
        self.source = source
        self.work_dir = getcwd()
        self.filename = None

    def update(self):
        self.extract()
        self.load()
        self.backup()

    def extract(self):
        self.filename = self.get_task()
        if not self.filename:
            print('No new files found')
            return
        xl = pd.ExcelFile(path.join(getcwd(), FILES_SOURCE_DIR, self.filename))
        df = pd.read_excel(path.join(getcwd(), FILES_SOURCE_DIR, self.filename), sheet_name=xl.sheet_names[0], header=0)
        sql_data = []
        print(self.filename, self.table_name)
        # TODO: Remove hardcode
        if self.table_name == 'TERMINALS':
            # FILL STG
            sql_query = """INSERT INTO de3at.paks_stg_terminals (
    terminal_id, terminal_type, terminal_city, terminal_address, stage_dt
) VALUES (?, ?, ?, ?, to_date(?, 'YYYY-MM-DD HH24:MI:SS'))
        """
            for index, row in df.iterrows():
                sql_data.append((
                    row['terminal_id'],
                    row['terminal_type'],
                    row['terminal_city'],
                    row['terminal_address'],
                    self.get_datetime_from_file(self.filename)
                ))
            if OUTPUT[0]:
                Connection.execute(sql_query, sql_data, many=True)
            else:
                print(sql_query)
                print(sql_data)
            # FILL STG_DEL
            sql_query = "DELETE FROM de3at.paks_stg_terminals_del WHERE 1=1"
            if OUTPUT[0]:
                Connection.execute(sql_query, many=True)
            else:
                print(sql_query)
            sql_query = """INSERT INTO de3at.paks_stg_terminals_del (terminal_id) VALUES (?)"""
            sql_data = [(item[0],) for item in sql_data]
            print(*sql_data[:5], sep='\n')
            if OUTPUT[0]:
                Connection.execute(sql_query, sql_data, many=True)
            else:
                print(sql_query)
                print(sql_data)

        elif self.table_name == 'PASSPORT_BLACKLIST':
            # FILL STG
            sql_query = """INSERT INTO DE3AT.PAKS_STG_PASSPORT_BLACKLIST (BLOCK_DATE, PASSPORT_NUM) 
            VALUES (TO_DATE(?, 'DD-MM-YYYY'), ?)"""
            sql_data = []
            for index, row in df.iterrows():
                sql_data.append((
                    row['date'].strftime('%d-%m-%Y'),
                    row['passport'],
                ))
            print(*sql_data[:10], sep='\n\n')
            if OUTPUT[0]:
                Connection.execute(sql_query, sql_data, many=True)
            else:
                print(sql_query)
                print(sql_data)

    def load(self):
        if self.mode.lower() == 'fact':
            filename = 'etl_fact_%s.sql' % self.table_name.lower()
        elif self.mode.lower() == 'scd2':
            filename = 'etl_dim_%s_hist.sql' % self.table_name.lower()
        else:
            print('MODE', self.mode.lower())
            raise IOError('Mode is not implemented')
        with open(path.join(getcwd(), SQL_SCRIPTS, filename), 'r') as fp:
            data = fp.read()
        sql_queries = data.split(';')
        if OUTPUT[0]:
            Connection.executemany(sql_queries[:-1])
        else:
            print(*sql_queries, end=';')

    def backup(self):
        if not path.exists(path.join(getcwd(), FILES_BACKUP_DIR)):
            makedirs(path.join(getcwd(), FILES_BACKUP_DIR))
        if self.filename:
            move(
                path.join(getcwd(), FILES_SOURCE_DIR, self.filename),
                path.join(getcwd(), FILES_BACKUP_DIR, '%s.backup' % self.filename)
            )
