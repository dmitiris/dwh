from datetime import datetime
from os import listdir, path, getcwd

from .dbconnect import Connection
from .environment import FILES_SOURCE_DIR, META, OUTPUT
from .sql_templates import CREATE_TABLE_TEMPLATE, SELECT_LAST_UPDATE, DROP_TABLE_TEMPLATE, \
    META_INSERT_TEMPLATE, META_DELETE_TEMPLATE


class Driver:
    def __init__(self,
                 mode,
                 table_name,
                 stg_columns,
                 trg_columns,
                 stg_schema,
                 src_columns=None,
                 mask=None
                 ):
        self.mask = mask
        self.mode = mode
        self.table_name = table_name
        self.trg_columns = trg_columns
        self.src_columns = src_columns
        self.stg_columns = stg_columns
        self.stg_schema = stg_schema

    @staticmethod
    def gen_table_name(name, layer, mode, error=False):
        # TODO: remove hardcoded prefix
        if layer == 'stg':
            table_name = 'PAKS_STG_%s' % name
        elif layer == 'del':
            table_name = 'PAKS_STG_%s_DEL' % name
        elif layer in ('dds', 'trg') and mode == 'scd1':
            table_name = 'PAKS_DWH_%s' % name
        elif layer in ('dds', 'trg') and mode == 'scd2':
            table_name = 'PAKS_DWH_DIM_%s_HIST' % name
        elif layer in ('dds', 'trg') and mode == 'fact':
            table_name = 'PAKS_DWH_FACT_%s' % name
        elif layer == 'report':
            table_name = 'PAKS_REP_%s' % name
        else:
            raise ValueError('Strange layer and mode: %s and %s' % (layer, mode))
        if len(table_name) > 30 and error:
            raise ValueError('Table name %s is too long: %s' % (table_name, len(table_name)))
        if len(table_name) > 30:
            return Driver.gen_table_name(
                ''.join([letter for letter in name if letter not in {'A', 'E', 'I', 'O', 'U'}]),
                layer, mode, True
            )
        else:
            return table_name

    def drop_table(self, layer):
        return DROP_TABLE_TEMPLATE % {
            'schema_name': self.stg_schema,
            'table_name': self.gen_table_name(self.table_name, layer, self.mode),
        }

    @staticmethod
    def meta_insert():
        return META_INSERT_TEMPLATE % {
            'schema_name': META.schema,
            'table_name': '%s%s' % (META.prefix, META.name)
        }

    @staticmethod
    def meta_remove():
        return META_DELETE_TEMPLATE % {
            'schema_name': META.schema,
            'table_name': '%s%s' % (META.prefix, META.name)
        }

    def init(self):
        if self.mode == 'report':
            sql_queries = [self.create_table('report'), ]
            if OUTPUT[0]:
                Connection.executemany(sql_queries, ignore=True)
            else:
                print(*sql_queries, sep='\n')
            return sql_queries
        sql_queries = [self.create_table('stg'), ]
        if self.mode == 'scd2':
            sql_queries.append(self.create_table('del'))
        sql_queries.append(self.create_table('trg'))
        if OUTPUT[0]:
            Connection.executemany(sql_queries, ignore=True)
        else:
            print(*sql_queries, sep='\n')
        sql_query = self.meta_insert()
        sql_data = (self.stg_schema, self.table_name, '1899-12-31 23:59:59')
        if OUTPUT[0]:
            Connection.execute(sql_query, sql_data, ignore=True)
        else:
            print(sql_query)
        return sql_queries

    def drop(self):
        if self.mode == 'report':
            sql_queries = [self.drop_table('report'), ]
            if OUTPUT[0]:
                Connection.executemany(sql_queries, ignore=True)
            else:
                print(*sql_queries, sep='\n')
            return sql_queries
        sql_queries = [self.drop_table('stg'), ]
        if self.mode == 'scd2':
            sql_queries.append(self.drop_table('del'))
        sql_queries.append(self.drop_table('trg'))
        if OUTPUT[0]:
            Connection.executemany(sql_queries, ignore=True)
        else:
            print(*sql_queries, sep='\n')
        sql_query = self.meta_remove()
        sql_data = (self.stg_schema, self.table_name)
        if OUTPUT[0]:
            Connection.execute(sql_query, sql_data, ignore=True)
        else:
            print(*sql_queries, sep='\n')
        return sql_queries

    def create_table(self, layer):
        if layer == 'stg':
            columns = list(self.stg_columns if self.stg_columns else self.src_columns)
            column_names = {column[0] for column in columns}
            if 'STAGE_DT' not in column_names:
                columns.append(['STAGE_DT', 'date'])
        elif layer == 'del':
            columns = list(self.stg_columns if self.stg_columns else self.src_columns)[:1]
        elif layer == 'trg':
            columns = list(
                self.trg_columns if self.trg_columns else self.stg_columns if self.stg_columns else self.src_columns
            )
            if self.mode == 'scd2':
                column_names = {column[0] for column in columns}
                if 'EFFECTIVE_FROM' not in column_names:
                    columns.append(['EFFECTIVE_FROM', 'date', {
                        'DEFAULT': "TO_DATE('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"
                    }])
                if 'EFFECTIVE_TO' not in column_names:
                    columns.append(['EFFECTIVE_TO', 'date', {
                                    'DEFAULT': "TO_DATE('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"
                    }])
                if 'EFFECTIVE_FROM' not in column_names:
                    columns.append(['DELETED_FLG', 'char(1)'])
        elif layer == 'report':
            columns = list(self.stg_columns if self.stg_columns else self.src_columns)
        else:
            raise IOError("Strange mode|layer: %s|%s" % (self.mode, layer))
        return CREATE_TABLE_TEMPLATE % {
                'schema_name': self.stg_schema,
                'table_name': self.gen_table_name(self.table_name, layer, self.mode),
                'columns': '\n    , '.join([' '.join([
                    item[0].upper(),
                    item[1],
                    '' if len(item) < 3 else 'DEFAULT %s' % (
                        item[2]['DEFAULT']) if item[2].get('DEFAULT') else '',
                    '' if len(item) < 3 else 'PRIMARY KEY %s' % (
                        item[2]['PRIMARY KEY']) if item[2].get('PRIMARY KEY') else '',
                ]) for item in columns])
            }

    @staticmethod
    def get_datetime_from_file(filename):
        # TODO: Remove hard code
        name_splitted = filename.split('_')
        file_datetime = '%s-%s-%s' % (
            name_splitted[-1][4:8],
            name_splitted[-1][2:4],
            name_splitted[-1][0:2]
        )
        return file_datetime

    def get_last_update(self):
        sql_query = SELECT_LAST_UPDATE % {
            'schema_name': META.schema,
            'table_name': '%s%s' % (META.prefix, META.name),
        }
        print(self.table_name)
        sql_data = (self.stg_schema, self.table_name)
        if OUTPUT[0]:
            result = Connection.execute(sql_query, sql_data, fetch=True)
        else:
            result = ''
        if len(result) > 0:
            result = datetime.strptime(result[0][0], '%Y-%m-%d %H:%M:%S')
        else:
            result = datetime.strptime('1899-01-01', '%Y-%m-%d')
        return result

    def get_task(self):
        files = listdir(path.join(getcwd(), FILES_SOURCE_DIR))
        min_date = datetime.strptime('2999-12-31', '%Y-%m-%d')
        max_date = self.get_last_update()
        target = None
        for filename in files:
            if self.table_name.upper() in filename.upper():
                date = datetime.strptime(self.get_datetime_from_file(filename), '%Y-%m-%d')
                if min_date > date > max_date:
                    min_date = date
                    target = filename
        return target
