## DWH ETL Process
### Requirements
* python 3.4+
* jaydebeapi - connecting to Oracle database
* pandas - reading xlsx files

### Implemented features
* Extracting from xlsx, plain format files (csv, txt, etc), OracleDB
* Loading to OracleDB
* Storage as Facts, SCD2

### Installation
1) git clone https://github.com/dmitiris/dwh.git
2) pip install -r requirements
3) Edit config.json.template as needed and rename it to config.json

### Usage
#### Initialization
    main.py [-h] [-m {execute,generate}] [-tn TABLE_NAME] [--meta] {init,drop,update,report}

##### Optional arguments 
    -h, --help  - shows help message and exit
    -m, --mode  - choose between modes:
            execute - executes SQL queries in database (DEFAULT)
            generate - generates SQL script in stdout
    -tn TABLE_NAME, --table-name TABLE_NAME - process only selected table names
    --meta  - process with meta (will drop meta_table)

##### Positional arguments
    init - generates/executes SQL create DDL, writes initial metadata into meta_table
    drop - generates/executes SQL drop DDL, removes metadata from meta_table
    update - generates/executes ETL process SQL queries
    report - generates/executes SQL queries for report

##### Examples:
    ./main.py -tn transactions init
creates meta_table if doesn't exist and then creates table transactions as specified in config.json

    ./main.py update
launches ETL process for all tables specified in config.json

    ./main.pu -tn cards -m generate --meta drop
generates SQL queries to drop meta_table and cards table and prints them in stdout