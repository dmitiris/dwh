# COMMON TEMPLATES
CREATE_TABLE_TEMPLATE = """CREATE TABLE %(schema_name)s.%(table_name)s (
    %(columns)s
)"""

DROP_TABLE_TEMPLATE = """DROP TABLE %(schema_name)s.%(table_name)s"""

INSERT_VALUES_TEMPLATE = """INSERT INTO %(schema_name)s.%(table_name)s (
    %(columns)s
) VALUES (%(values)s)"""

INSERT_TABLE_TEMPLATE = """ INSERT INTO %(to_schema)s.%(to_table)s (%(to_columns)s) 
SELECT %(from_columns)s FROM %(from_schema)s.%(from_table)s"""

# META TEMPLATES
META_INSERT_TEMPLATE = """INSERT INTO %(schema_name)s.%(table_name)s (SCHEMA_NAME, TABLE_NAME, LAST_UPDATE_DT)
VALUES (?, ?, TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS'))"""

META_UPDATE_TEMPLATE = """UPDATE %(schema_name)s.%(table_name)s SET LAST_UPDATE_DT = TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')
WHERE SCHEMA_NAME=? AND TABLE_NAME=?"""

META_DELETE_TEMPLATE = """DELETE FROM %(schema_name)s.%(table_name)s
WHERE SCHEMA_NAME=? AND TABLE_NAME=?"""

SELECT_LAST_UPDATE = """SELECT LAST_UPDATE_DT FROM %(schema_name)s.%(table_name)s
WHERE SCHEMA_NAME=? AND TABLE_NAME=?"""


