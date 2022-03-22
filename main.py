#!/opt/Software/Anaconda3/bin/python
import shutil
from argparse import ArgumentParser
from os import path, getcwd, listdir

from py_scripts.model import Model
from py_scripts.environment import CONFIG, META, OUTPUT, FILES_SOURCE_DIR
from py_scripts.dbconnect import Connection


def main(action, mode, meta, selected_tables):
    if action in {'revert'}:
        files = listdir('_data_backup')
        for fname in files:
            shutil.copy(
                path.join(getcwd(), '_data_backup', fname),
                path.join(getcwd(), FILES_SOURCE_DIR, fname),
            )
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
    if action in {'report', 'update'}:
        reports = CONFIG.get('REPORTS')
        if reports:
            reports = {key.upper(): reports[key] for key in reports}
            for report in reports.keys():
                report = Model(report, reports[report], output=mode)
                report.init()
                report.update()
    if action in {'drop'} and not selected_tables:
        reports = CONFIG.get('REPORTS')
        if reports:
            reports = {key.upper(): reports[key] for key in reports}
            for report in reports.keys():
                report = Model(report, reports[report], output=mode)
                report.drop()


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
