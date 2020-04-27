import string
import yaml
import argparse
import os
import shutil
import sys
import pprint
import logging as lg
from py_dbutils.rdbms import postgres
import pyarrow
import random
from faker import Faker
import re
from collections import OrderedDict
import utils as utils
lg.basicConfig()
logging = lg.getLogger('fake-me-some')
 



def set_log_level(debug_level):
    if debug_level == 'DEBUG':
        logging.setLevel(lg.DEBUG)
    if debug_level == 'INFO':
        logging.setLevel(lg.INFO)
    if debug_level == 'WARN':
        logging.setLevel(lg.WARN)
    if debug_level == 'ERROR':
        logging.setLevel(lg.ERROR)
# run through the yaml and replace embedded params


def pre_process_yaml(yaml_file):
    # yaml_file = os.path.abspath(yaml_file)
    yaml_data = None
    with open(yaml_file, "r") as f:
        yaml_data = yaml.safe_load((f))

    source_db = yaml_data['db']['connection']
    src_db = None
    if source_db['type'] == 'POSTGRES':
        src_db = postgres.DB(host=source_db['host'],
                             port=source_db['port'],
                             dbname=source_db['db'],
                             schema=source_db['schema'],
                             userid=source_db['userid'],
                             pwd=os.environ.get(source_db['password_envir_var'], None))
    else:
        print('Error Non Supported Database: {}'.format(source_db['type']))
        sys.exit(1)

    return yaml_data, src_db


fake = Faker()
fake.add_provider('providers.lorem.sentence')
fake.add_provider('providers.lorem.words')
fake.add_provider('providers.lorem.word')
sentence = getattr(fake, 'sentence')
words = getattr(fake, 'words')
word = getattr(fake, 'word')
random_num = random.SystemRandom()


def random_char_generator(str_size=1):
    string_word = 'need to implement random char generator'
    return string_word


def random_string_generator(str_size, num_words=1):

    #allowed_chars = chars = string.ascii_letters + string.punctuation

    string_word = words(1)
    string_word = ' '.join(string_word)
    if len(string_word) > str_size:
        string_word = string_word[:str_size]
    return string_word


def fake_data(data_type):

    dynamic_module_path = "faker.{}"
    module = None
    func_name = None
    if len(data_type.split('.')) > 1:
        module = data_type.split('.')[0]
        func_name = data_type.split('.')[-1]
    else:
        if len(data_type.split(',')) > 1:
            return random_string_generator
        else:
            return random_string_generator
    if module is not None:

        dynamic_module_path = dynamic_module_path.format(module)

        module = __import__(dynamic_module_path)

        fake = Faker()
        fake.add_provider(module)
        func_name = getattr(fake, func_name)

        return func_name

    raise Exception("Fake Function Not found")


def map_fake_functions(root, yaml_data):
    import copy

    tables = copy.deepcopy(yaml_data[root])

    for tbl in tables.keys():

        t = tables[tbl]
        if t is not None:
            for col in t.keys():
                column_type = t[col] 
                if str(column_type).startswith('providers.'):
                    xx = fake_data(column_type)
                    t[col] = xx 

                elif (str(column_type).upper().startswith('NUMERIC')
                        or str(column_type).upper().startswith('DOUBLE')
                        or str(column_type).upper().startswith('MONEY')

                      ):

                    def rnd_float(start=0, end_max=sys.maxsize):
                        key_num = round(random.random(), 2)
                        return key_num
                    t[col] = rnd_float
                elif str(column_type).upper() == ('DATE'):
                    import datetime
                    def rnd_time():

                        return datetime.datetime.now().strftime("%Y-%m-%d")
                    t[col] = rnd_time
                elif str(column_type).upper().startswith('TIMESTAMP') or str(column_type).upper().startswith('DATETIME'):
                    import datetime

                    def rnd_time():
                        return datetime.datetime.now()
                    t[col] = rnd_time
                elif str(column_type).upper().startswith('VARCHAR') or str(column_type).upper().startswith('CHAR') or str(column_type).upper().startswith('TEXT'):

                    # get the len between parentasis
                    str_len = 0

                    try:
                        str_len = int(
                            re.search(r'\((.*?)\)', str(column_type).upper()).group(1))

                        def rnd_str(int_len=str_len):

                            return random_string_generator(int_len, int(int_len/6)+1)

                        t[col] = rnd_str
                    except:
                        logging.info("Not lenth specified assumes text")
                        fake = Faker()
                        fake.add_provider('providers.lorem.sentence')
                        sentence = getattr(fake, 'sentence')

                        def rnd_lorem():
                            return sentence()
                        t[col] = rnd_lorem

                elif str(column_type).upper() in ['BIGINT', 'INT', 'INTEGER']:

                    def rnd_int(start=0, end_max=sys.maxsize):

                        return random_num.randint(0, 65045)
                    t[col] = rnd_int
                elif str(column_type).upper() in ['SMALLINT']:

                    def rnd_int(start=0, end_max=sys.maxsize):

                        return random_num.randint(0, 255)
                    t[col] = rnd_int
                elif str(column_type).upper().startswith('BIT') or str(column_type).upper().startswith('BOOL'):

                    def rnd_bit(start=0, end_max=sys.maxsize):
                        return str(random.getrandbits(1))
                    t[col] = rnd_bit
                else:
                    raise Exception(
                        "Uknown type {}-{}".format(col, str(column_type)))

    return tables
# leave what's already in the yaml file there and add in what's new



def parse_cli_args():
    parser = argparse.ArgumentParser(prog='fake_me_some', usage="""%(prog)s [options]
    MAKE A config.yaml like this if you don't have one:
    db:
    connection: 
        db: postgres
        host: pgdb
        port: 5432
        type: 'POSTGRES'
        schema: "test"
        userid: 'docker'
        password_envir_var: PGPASSWORD """)

    

    parser.add_argument('--of','--outfile', default=None,
                        help='new configuration yaml file to dump to with table description')
    parser.add_argument('--rows', default=10,
                        help='Number of Rows to Fake')
    parser.add_argument('--o', default='CSV',
                        help='output data to CSV, DB, PARQUET')
    parser.add_argument('--d','--dir',default=None,
                        help='output_directory')
    parser.add_argument('--ll', default='INFO',
                        help='Logging mode: DEBUG INFO WARN ERROR')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--y','--yaml',  
                        help='path to yaml file')
    group.add_argument('--generate',
                        help='Generate YamlConfig Template')
    args = parser.parse_args()
    if args.generate:
        print("Generated Config Yaml template")
        utils.create_yaml(args.generate)
        sys.exit(0)


    kwargs={}
         
    kwargs['out_path'] = os.path.abspath(args.d)
    kwargs['yaml_file'] = os.path.abspath(args.y)
    kwargs['out_yaml_file'] = os.path.abspath(args.of)
    kwargs['out_format'] = os.path.abspath(args.o)
    kwargs['logging'] = os.path.abspath(args.ll)
    kwargs['rows'] = os.path.abspath(args.rows)
    
 
 
    return kwargs


def fake_some_data_parquet(file_path, table, num_rows):
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    # make row for each
    rows = []
    for _ in range(num_rows):
        row = []
        for col in table.keys():
            data = table[col]()
            row.append(data)
        rows.append(row)
    header = [col for col in table.keys()]

    df = pd.DataFrame.from_records(rows, columns=header)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)


def fake_some_data_db(table_name, table, num_rows, db_conn):
    import pandas as pd
    # make row for each
    rows = []
    logging.info("Faking Table: {} - {} Rows".format(table_name, num_rows))
    for _ in range(num_rows):
        row = []
        for col in table.keys():
            data = table[col]()
            row.append(data)
        rows.append(row)
    header = [col for col in table.keys()]

    pd = pd.DataFrame.from_records(rows, columns=header)
    engine = db_conn.connect_SqlAlchemy()

    pd.to_sql(table_name, engine, if_exists='append',
              index=False, schema=db_conn.schema)



def fake_some_data_csv(file_path, table, num_rows):

    # make row for each
    rows = []
    for _ in range(num_rows):
        row = []
        for col in table.keys():

            data = table[col]()
            row.append(data)
        rows.append(row)
    header = [col for col in table.keys()]
    import csv

    with open(file_path, 'w') as f:
        print("writing file: ", os.path.abspath(file_path))
        wr = csv.writer(f)
        wr.writerow(header)
        wr.writerows(rows)
def do_work(kwargs):
 
    out_path=kwargs['out_path']  
    yaml_file=kwargs['yaml_file']  
    out_yaml_file=kwargs['out_yaml_file']  
    output=kwargs.get('out_format',None)  
    log_level=kwargs.get('logging','DEBUG') 
    rows=kwargs.get('rows',10)
    set_log_level(log_level)
    
    with open(yaml_file) as f:
        #yaml_data = yaml.safe_load((f))
        yaml_data = utils.ordered_load(f, yaml.SafeLoader)

    logging.info('Read YAML file: \n\t\t{}'.format(yaml_file))
    
    yaml_dict, db_conn = pre_process_yaml(yaml_file)
    if out_yaml_file is not None:

        if output == 'SUGGEST':

            utils.generate_yaml_from_db_suggest(db_conn, out_yaml_file, yaml_data)
        else:
            utils.generate_yaml_from_db(db_conn, out_yaml_file, yaml_data)
    else:
        # map each column to a faker function

        tables = map_fake_functions('Tables', yaml_dict)
        for table in tables.keys():

            t = tables[table]

            if t is not None:
                if output == 'CSV':
                    print("OUTPUT TO CSV:")
                    fake_some_data_csv(os.path.join(
                        out_path, table+'.csv'), t, int(rows))
                elif output == 'PARQUET':
                    fake_some_data_parquet(os.path.join(
                        out_path, table+'.parquet'), t, int(rows))
                elif output == 'DB':
                    print("OUTPUT TO DATABASE:")
                    fake_some_data_db(table, t, int(rows), db_conn)
                else:
                    print("unknow output so skipping table: {}".format(table))

def main(**kwargs):
    # process_list = [] 
    if not kwargs.get('yaml_file',None):
        kwargs = parse_cli_args()
        
    do_work(kwargs)
     

if __name__ == '__main__':
    utils.printhello()
    main()
