from .sql_templates import CREATE_TABLE_TEMPLATE, DROP_TABLE_TEMPLATE


class Meta:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name', 'META_DATA1').upper()
        self.prefix = kwargs.get('prefix', '').upper()
        if len(self.prefix) and self.prefix[-1] != '_':
            self.prefix = '%s_' % self.prefix
        self.schema = kwargs.get('schema', 'ADMIN').upper()
        self.full_name = '%s.%s%s' % (self.schema, self.prefix, self.name)
        self.mode = kwargs.get('mode')
        self.source = kwargs.get('source')
        self.columns = kwargs.get(
            'columns',
            [
                ('SCHEMA_NAME', 'VARCHAR2(30)'),
                ('TABLE_NAME', 'VARCHAR2(30)'),
                ('LAST_UPDATE_DT', 'DATE', {'DEFAULT': "TO_DATE('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"}),
                ('CONSTRAINT', 'PK_PAKS_META_DATA', {'PRIMARY KEY':  '(SCHEMA_NAME, TABLE_NAME)'})
            ]
        )

    def init(self):
        return CREATE_TABLE_TEMPLATE % {
            'schema_name': self.schema,
            'table_name': '%s%s' % (self.prefix, self.name),
            'columns': '\n    , '.join([' '.join([
                item[0].upper(),
                item[1],
                '' if len(item) < 3 else 'DEFAULT %s' % (
                    item[2]['DEFAULT']) if item[2].get('DEFAULT') else '',
                '' if len(item) < 3 else 'PRIMARY KEY %s' % (
                    item[2]['PRIMARY KEY']) if item[2].get('PRIMARY KEY') else '',
            ]) for item in self.columns])
        }

    def drop(self):
        return DROP_TABLE_TEMPLATE % {
            'schema_name': self.schema,
            'table_name': '%s%s' % (self.prefix, self.name)
        }
