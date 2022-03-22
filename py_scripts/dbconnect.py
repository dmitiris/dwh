from jaydebeapi import connect, DatabaseError

from .environment import DRIVER, CONN_STR, DBUSER, DBWORD, JDBC_JAR


class Connection:
    driver = DRIVER
    conn_string = CONN_STR
    user = DBUSER
    password = DBWORD
    jdbc_jar = JDBC_JAR

    @staticmethod
    def execute(sql_query, sql_data=None, fetch=False, many=False, ignore=False):
        with connect(Connection.driver,
                     Connection.conn_string,
                     [Connection.user, Connection.password],
                     Connection.jdbc_jar) as conn:
            conn.jconn.setAutoCommit(False)
            cur = conn.cursor()
            if sql_data and many:
                cur.executemany(sql_query, sql_data)
            elif sql_data and ignore:
                try:
                    cur.execute(sql_query, sql_data)
                    print('\n%s\n%s\nSTATUS: QUERY EXECUTED' % (sql_query, sql_data))
                except DatabaseError as e:
                    print('\n%s\n%s\nSTATUS: FINISHED WITH ERROR: %s' % (sql_query, sql_data, e))
            elif sql_data:
                cur.execute(sql_query, sql_data)
                print('\n%s\n%s\nSTATUS: QUERY EXECUTED' % (sql_query, sql_data))
            elif ignore:
                try:
                    cur.execute(sql_query)
                    print('\n%s\nSTATUS: QUERY EXECUTED' % sql_query)
                except DatabaseError as e:
                    print('\n%s\nSTATUS: FINISHED WITH ERROR: %s' % (sql_query, e))
            else:
                cur.execute(sql_query)
                print('\n%s\nSTATUS: QUERY EXECUTED' % sql_query)
            if fetch:
                data = cur.fetchall()
            else:
                data = None
            conn.commit()
        return data

    @staticmethod
    def executemany(queries, ignore=False):
        with connect(Connection.driver,
                     Connection.conn_string,
                     [Connection.user, Connection.password],
                     Connection.jdbc_jar) as conn:
            conn.jconn.setAutoCommit(False)
            cur = conn.cursor()
            for query in queries:
                if ignore:
                    try:
                        cur.execute(query)
                        print('\n%s\nSTATUS: QUERY EXECUTED' % query)
                    except DatabaseError as e:
                        print('\n%s\nSTATUS: FINISHED WITH ERROR: %s' % (query, e))
                else:
                    print(query)
                    cur.execute(query)
            conn.commit()
