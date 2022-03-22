from os import getcwd, path

from .dbconnect import Connection
from .driver import Driver
from .environment import SQL_SCRIPTS, OUTPUT


class Oracle(Driver):
    def __init__(self, table_name, mode, output, src_schema=None, src_columns=None, stg_schema=None, stg_columns=None,
                 trg_schema=None, trg_columns=None, source=None):
        super().__init__(mode, table_name, src_columns, stg_columns, trg_columns, stg_schema, trg_schema)
        self.table_name = table_name.upper()
        self.mode = mode
        self.output = output
        self.src_schema = src_schema.upper() if src_schema else src_schema
        self.src_columns = src_columns
        self.stg_schema = stg_schema.upper() if stg_schema else stg_schema
        self.stg_columns = stg_columns
        self.trg_schema = trg_schema.upper() if trg_schema else trg_schema
        self.trg_columns = trg_columns
        self.source = source
        self.file_datetime = None

    def update(self):
        if self.mode.lower() == 'fact':
            filename = 'etl_fact_%s.sql' % self.table_name.lower()
        elif self.mode.lower() == 'scd2':
            filename = 'etl_dim_%s_hist.sql' % self.table_name.lower()
        elif self.mode.lower() == 'report':
            filename = 'rep_%s.sql' % self.table_name.lower()
        else:
            print('MODE', self.mode.lower())
            raise IOError('Mode is not implemented')
        with open(path.join(getcwd(), SQL_SCRIPTS, filename), 'r') as fp:
            data = fp.read()
        queries = data.split(';')
        if OUTPUT[0]:
            Connection.executemany(queries[:-1])
        else:
            print(*queries, sep=';')
