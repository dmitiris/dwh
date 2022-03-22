from datetime import datetime
from os import path, getcwd, makedirs
from shutil import move

from .dbconnect import Connection
from .driver import Driver
from .environment import FILES_SOURCE_DIR, OUTPUT, SQL_SCRIPTS, FILES_BACKUP_DIR
from .sql_templates import INSERT_VALUES_TEMPLATE


class TXT(Driver):
    def __init__(self, table_name, mode, mask, output, src_columns=None, stg_schema=None, stg_columns=None,
                 trg_schema=None, trg_columns=None, source=None, delimiter=';', line_break='\n'):
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
        self.file_datetime = None
        self.delimiter = delimiter
        self.line_break = line_break
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
        columns = list(self.stg_columns if self.stg_columns else self.src_columns)
        column_names = []
        values = []

        numbers = []  # list of numeric column indexes, required to check if data is numeric while extracting
        stage_dt_trg = False  # indicates if there is "stage_dt" service column in source, or it should be added

        for i in range(len(columns)):
            # column_data from config.json:
            # [0]NAME [1]TYPE [2]{CONSTRAINTS, FORMATS}
            column_data = columns[i]
            if column_data[0].upper() == 'STAGE_DT':
                stage_dt_trg = True
            column_names.append(column_data[0].upper())
            if column_data[1].upper() == 'DATE':
                if len(column_data) > 2 and column_data[2].get('FORMAT'):
                    values.append("TO_DATE(?, '%s')" % column_data[2].get('FORMAT'))
                elif len(column_data) > 2 and column_data[2].get('format'):
                    values.append("TO_DATE(?, '%s')" % column_data[2].get('format'))
                else:
                    values.append("TO_DATE(?, '%s')" % 'YYYY-MM-DD')
            else:
                values.append('?')
            if column_data[1].upper() in ('INTEGER', ) or column_data[1].upper().startswith('NUMERIC'):
                numbers.append(i)

        if not stage_dt_trg:
            column_names.append('STAGE_DT')
            values.append("TO_DATE(?, '%s')" % 'YYYY-MM-DD')

        # building query template
        sql_query = INSERT_VALUES_TEMPLATE % {
            'schema_name': self.stg_schema,
            'table_name': self.gen_table_name(self.table_name, 'stg', self.mode),
            'columns': ', '.join(column_names),
            'values': ', '.join(values)
        }

        # reading data from file
        with open(path.join(getcwd(), FILES_SOURCE_DIR, self.filename), 'r') as fp:
            data = fp.read()
        data = data.split(self.line_break)
        sql_data = []
        # stage_dt from filename
        stage_dt = self.get_datetime_from_file(self.filename)
        if not stage_dt:
            stage_dt = datetime.now().strftime('%Y-%m-%d')  # stage_dt as today

        # reading file
        for line in data[1:]:
            row = line.split(self.delimiter)
            if isinstance(row, list) and len(row) > 1:
                # cleaning numbers
                for num in numbers:
                    row[num] = row[num].replace(',', '.')
                # appending stage_dt value if there is no stage_dt in file
                if not stage_dt_trg:
                    row.append(stage_dt)
                sql_data.append(row)
        if OUTPUT[0]:
            Connection.execute(sql_query, sql_data, many=True)
        else:
            ''

    # TODO: WRITE SQL GENERATOR
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
        queries = data.split(';')
        if OUTPUT[0]:
            Connection.executemany(queries[:-1])
        else:
            print(*queries, sep=';')

    def backup(self):
        if not path.exists(path.join(getcwd(), FILES_BACKUP_DIR)):
            makedirs(path.join(getcwd(), FILES_BACKUP_DIR))
        if self.filename:
            move(
                path.join(getcwd(), FILES_SOURCE_DIR, self.filename),
                path.join(getcwd(), FILES_BACKUP_DIR, '%s.backup' % self.filename)
            )
