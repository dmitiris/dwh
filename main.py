#!/opt/Software/Anaconda3/bin/python
import shutil
from argparse import ArgumentParser
from os import path, getcwd, listdir

from py_scripts.model import Model
from py_scripts.environment import CONFIG, META, OUTPUT, FILES_SOURCE_DIR

# def drop():
#     # queries = [
#     #     "DROP TABLE DE3AT.PAKS_DWH_DIM_TERMINALS_HIST",
#     #     "DROP TABLE DE3AT.PAKS_STG_TERMINALS",
#     #     "DROP TABLE DE3AT.PAKS_STG_TERMINALS_DEL",
#     #     "DROP TABLE DE3AT.PAKS_META_DATA",
#     # ]
#     # queries = [
#     #     "DROP TABLE DE3AT.PAKS_SRC_CARDS",
#     #     "DROP TABLE DE3AT.PAKS_STG_CARDS",
#     #     "DROP TABLE DE3AT.PAKS_STG_CARDS_DEL",
#     #     "DROP TABLE DE3AT.PAKS_DWH_DIM_CARDS_HIST",
#     #     "DROP TABLE DE3AT.PAKS_META_DATA",
#     # ]
#     # queries = [
#     #     "DROP TABLE DE3AT.PAKS_SRC_ACCOUNTS",
#     #     "DROP TABLE DE3AT.PAKS_STG_ACCOUNTS",
#     #     "DROP TABLE DE3AT.PAKS_STG_ACCOUNTS_DEL",
#     #     "DROP TABLE DE3AT.PAKS_DWH_DIM_ACCOUNTS_HIST",
#     #     "DROP TABLE DE3AT.PAKS_SRC_CLIENTS",
#     #     "DROP TABLE DE3AT.PAKS_STG_CLIENTS",
#     #     "DROP TABLE DE3AT.PAKS_STG_CLIENTS_DEL",
#     #     "DROP TABLE DE3AT.PAKS_DWH_DIM_CLIENTS_HIST",
#     #     "DROP TABLE DE3AT.PAKS_META_DATA",
#     # ]
#     Connection.executemany(DROP_SQL, ignore=True)
#     print('Tables dropped')
#
#
# def init():
#     drop()
#     # queries = [
#     #     # "CREATE TABLE DE3AT.PAKS_SRC_CARDS AS SELECT * FROM BANK.CARDS",
#     #     STG_ACCOUNTS, STG_ACCOUNTS_DEL, DWH_DIM_ACCOUNTS_HIST,
#     #     STG_CLIENTS, STG_CLIENTS_DEL, DWH_DIM_CLIENTS_HIST,
#     #     # STG_CARDS, STG_CARDS_DEL, DWH_DIM_CARDS_HIST,
#     #
#     #     # STG_TERMINALS, STG_TERMINALS_DEL, DWH_DIM_TERMINALS_HIST
#     #     META_DATA
#     # ]
#     Connection.executemany(INIT_SQL, ignore=True)
#     print('Tables created')
#     Connection.executemany(META_INIT)
#     print('Meta filled')
#
#
# def update(count):
#     # model = DB('scd2', 'cards')
#     if count == 1:
#         with open(path.join('sql_scripts', 'etl_dim_accounts_hist.sql'), 'r') as fp:
#             data = fp.read()
#             queries = data.split(';')
#         Connection.executemany(queries[:-1])
#
#     # if count == 1:
#     #     with open(path.join('sql_scripts', 'etl_dim_cards_hist.sql'), 'r') as fp:
#     #         data = fp.read()
#     #         queries = data.split(';')
#     #     Connection.executemany(queries[:-1])
#     # elif count == 'test':
#     #     with open(path.join('sql_scripts', 'etl_dim_cards_hist_tbd.sql'), 'r') as fp:
#     #         data = fp.read()
#     #         queries = data.split(';')
#     #     Connection.executemany(queries[:-1])
#     # elif count == 2:
#     #     queries = [
#     #         "INSERT INTO DE3AT.PAKS_SRC_CARDS(CARD_NUM, ACCOUNT, CREATE_DT, UPDATE_DT) "
#     #         "VALUES('2227 2035 3892 4389', '40817810437543724523', CURRENT_DATE, NULL)",
#     #         "UPDATE DE3AT.PAKS_SRC_CARDS SET ACCOUNT = '40817810437543724522', UPDATE_DT = CURRENT_DATE "
#     #         "WHERE CARD_NUM = '5987 6550 1209 8987'",
#     #         "DELETE FROM DE3AT.PAKS_SRC_CARDS WHERE CARD_NUM = '2584 2622 5927 1055'"
#     #     ]
#     #     Connection.executemany(queries)
#     # elif count == 3:
#     #     queries = [
#     #         "DELETE FROM DE3AT.PAKS_SRC_CARDS WHERE CARD_NUM = '5987 6550 1209 8987'",
#     #         "UPDATE DE3AT.PAKS_SRC_CARDS SET ACCOUNT = '40817810437543724555', UPDATE_DT = CURRENT_DATE "
#     #         "WHERE CARD_NUM = '40817810437543724523'",
#     #     ]
#     #     Connection.executemany(queries)
#     # if count == 1:
#     #     model = XLSX('scd2', path.join('data', 'terminals_01032021.xlsx'))
#     # elif count == 2:
#     #     model = XLSX('scd2', path.join('data', 'terminals_02032021.xlsx'))
#     # elif count == 3:
#     #     model = XLSX('scd2', path.join('data', 'terminals_03032021.xlsx'))
#     # try:
#     #     model.read()
#     # except ValueError:
#     #     pass
#     # with open(path.join('sql_scripts', 'etl_dim_terminals_hist.sql'), 'r') as fp:
#     #     data = fp.read()
#     #     queries = data.split(';')
#     # Connection.executemany(queries[:-1])
#
#
# def rubbish():
#     # queries = [
#     #     STG_TERMINALS, STG_TERMINALS_DEL, DWH_DIM_TERMINALS_HIST
#     # ]
#     # for query in queries:
#     #     try:
#     #         Connection.execute(query)
#     #     except DatabaseError:
#     #         pass
#     # # Connection.executemany(INIT_SQL)
#     # # Connection.executemany(DROP_SQL)
#     # # Connection.execute(DWH_FACT_PASSPORT_BLACKLIST)
#     # Connection.execute(META_DATA)
#     # Connection.executemany(META_INIT)
#     model = TXT('fact', path.join('data', 'transactions_03032021.txt'))
#     model.read()
#     # model = XLSX('scd2', path.join('data', 'terminals_03032021.xlsx'))
#     # model.read()
#     with open(path.join('sql_scripts', 'etl_dim_terminals_hist.sql'), 'r') as fp:
#         data = fp.read()
#         queries = data.split(';')
#     Connection.executemany(queries[:-1])


# def get_mask():
#     return {
#         'passport_blacklist': ('passport_blacklist_%(date)s.xlsx', '%d%m%Y'),
#         'transactions': ('transactions_%(date)s.txt', '%d%m%Y'),
#         'terminals': ('terminals_%(date)s.xlsx', '%d%m%Y'),
#     }


# def get_task(min_date):
#     files = listdir(FILES)
#     mask = get_mask()
#     files_dated = {}
#     min_date = datetime.strptime('29991231', '%Y%m%d')
#     for filename in files:
#         parts = filename.split('_')
#         date_string = parts[-1].split('.')[0]
#         date = datetime.strptime(date_string, '%d%m%Y')
#         # Не знаю зачем мне все имена файлов и даты, но пусть будет так
#         if date < min_date:
#             min_date = date
#         if date in files_dated:
#             files_dated[date].append(filename)
#         else:
#             files_dated[date] = [filename, ]
#     return files_dated.get(min_date)
from py_scripts.meta import Meta
from py_scripts.dbconnect import Connection


def main(action, mode, meta, selected_tables):
    if action in {'revert'}:
        files = listdir('_data_backup')
        for fname in files:
            shutil.copy(
                path.join(getcwd(), '_data_backup', fname),
                path.join(getcwd(), FILES_SOURCE_DIR, fname),
            )
    if action in {'report', 'update'}:
        pass
    if action not in {'report', }:
        # Meta table
        if meta and action == 'drop':
            sql_query = META.drop()
        elif action == 'init':
            sql_query = META.init()
        else:
            sql_query = None
        if OUTPUT[0] and sql_query:
            Connection.execute(sql_query, ignore=True)
        elif sql_query:
            print(sql_query)
        # Data tables
        tables = CONFIG.get('TABLES')
        if tables:
            tables = {key.upper(): tables[key] for key in tables}
        else:
            raise IOError('No "TABLES" section in config.json')
        if selected_tables:
            selected_tables = [table.upper() for table in selected_tables]
        else:
            selected_tables = tables.keys()

        for table_name in selected_tables:
            if table_name in tables:
                table = Model(table_name, tables[table_name], output=mode)
                if action == 'init':
                    table.init()
                elif action == 'update':
                    table.update()
                elif action == 'drop':
                    table.drop()
            else:
                print('There is no "%s" table in config.json' % table_name)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-m', '--mode', choices=['execute', 'generate'], default='generate')
    parser.add_argument('-tn', '--table-name', action='append')
    parser.add_argument('--meta', action='store_true')
    parser.add_argument('action', choices=['init', 'drop', 'update', 'report', 'increment', 'revert'])
    args = parser.parse_args()
    print(args)
    if args.mode == 'generate':
        OUTPUT.append(False)
    elif args.mode == 'execute':
        OUTPUT.append(True)
    main(args.action, args.mode, args.meta, args.table_name)
