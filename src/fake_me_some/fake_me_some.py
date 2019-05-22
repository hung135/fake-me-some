import yaml 
import argparse
import os
import shutil  
import sys
import pprint
import logging as lg  
from py_dbutils.rdbms import postgres 
import pyarrow
  
lg.basicConfig()
logging = lg.getLogger()
# logging.setLevel(lg.INFO)
logging.setLevel(lg.INFO)
  
WORKINGPATH=os.environ.get('WORKINGPATH',None)

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
    yaml_data = yaml.full_load(open(yaml_file))
    
    source_db = yaml_data['db']['connection']
    src_db=None
    if source_db['type']=='POSTGRES':
        src_db = postgres.DB(host=source_db['host'],
                                        port=source_db['port'],
                                        dbname=source_db['db'],
                                        schema=source_db['schema'],
                                        userid=source_db['userid'],
                                        pwd=os.environ.get(source_db['password_envir_var'],None) )
    else:
        print('Error Non Supported Database: {}'.format(source_db['type']))
        sys.exit(1)
    
    return yaml_data, src_db

import random
import string

def random_string_generator(str_size, allowed_chars=None):
    if allowed_chars is None:
        allowed_chars=chars = string.ascii_letters + string.punctuation
    return ''.join(random.choice(allowed_chars) for x in range(str_size))

    
#function to derive a function to generate data and return that function to be called later
def fake_data(data_type):
    from faker import Faker
    dynamic_module_path="faker.providers.{}"
    module=None
    func_name=None
    if len(data_type.split('.'))>1:
        module=data_type.split('.')[0]
        func_name=data_type.split('.')[1]
    else:
        if len(data_type.split(','))>1:
            return random_string_generator
        else:
            return random_string_generator
    if module is not None:
        
        dynamic_module_path=dynamic_module_path.format(module)
        
        module = __import__(dynamic_module_path)
         
        fake = Faker()
        fake.add_provider(module)
        func_name = getattr(fake, func_name)
        
    return func_name
    
def map_fake_functions(root,yaml_data): 
    import copy
    tables = copy.deepcopy(yaml_data[root])
    
    for tbl in tables.keys():
         
        t=tables[tbl]
        if t is not None:
            for col in t.keys():
                
                if len(t[col].split('.'))>1:
                    xx=fake_data(t[col])
                    t[col]=xx
 
                else:
                    if len(t[col].split(','))>1:
                        str_len=int(t[col].split(',')[1])
                        def rnd_str(int_len=str_len):
                            return random_string_generator(str_len,None)
                        t[col]=rnd_str
                        
                    else:
                        def rnd_int(start=0,end_max=sys.maxsize):
                            key_num = random.SystemRandom()
                            return key_num.randint(0, 65045)
                             
                        t[col]=rnd_int
        else:
            #pull skip table
            pass
 
    return tables
#leave what's already in the yaml file there and add in what's new
def merge_dict_file(tables,file,yaml_data):
    
    root='Tables'    
    has_root=False
    file_yaml=None
    db=yaml_data['db']
    with open(file, 'r') as outfile:
        file_yaml=yaml.full_load(open(file))
        if file_yaml.get(root,None):
            has_root=True
    if not has_root:
        with open(file, 'a') as outfile:
            yaml.dump(tables, outfile, default_flow_style=False)
    else:
        #loop through every tables found in DB
        for tbl in tables[root].keys():
            t=tables[root][tbl]
            #check to see if table is in yaml file
            #if not add everything in
            file_yaml_tbl=file_yaml[root].get(tbl,None)
            if file_yaml_tbl is None:
                print("addding to yaml ",tbl)
                file_yaml[root][tbl]=t
                
            else:           
                # since table exist loop through each column
                for col in t.keys():
                    #if column doesn't exist in yaml add the column
                    if file_yaml[root][tbl].get(col,None) is None:
                        file_yaml[root][tbl][col]=t[col]

        if file_yaml.get('db',None) is None:
            file_yaml['db']=db
        with open(file, 'w') as outfile:
            yaml.dump(file_yaml, outfile, default_flow_style=False)
        
def generate_yaml_from_db(db_conn,file_fqn,yaml_data):

    fqn=os.path.abspath(file_fqn)
    table_list=db_conn.get_all_tables()
    tbl={}
     
    for t in table_list:
        if t.startswith(db_conn.schema+'.'):
           
            cols=get_table_column_types(db_conn,t)
            tbl[str(db_conn.dbschema+"."+t)]=cols
    tables={"Tables":tbl}
   
    
    
    if os.path.isfile(fqn):
        print("File Already Exists Merging Updates")
        merge_dict_file(tables,fqn,yaml_data)
    else:
        with open(fqn, 'w') as outfile:
            yaml.dump(tables, outfile, default_flow_style=False)
     

def parse_cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--yaml', default='config.yaml', help='path to yaml file')
    parser.add_argument('--generate_yaml',default=None,help='file name of yaml to create queries database for tables and columns')
    parser.add_argument('--num_rows', default=10,help='Prints out cardinality of analyzeed columns')
    parser.add_argument('--output',default='CSV',help='output data to CSV, DB, PARQUET')
    parser.add_argument('--log_level', default='INFO',
                        help='Default Log Level')
    args = parser.parse_args()
    return args

def fake_some_data_parquet(file_path,table,num_rows):
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
 
    #make row for each 
    rows=[]
    for _ in range(num_rows):
        row=[]
        for col in table.keys():
            data=table[col]()
            row.append(data)
        rows.append(row)
    header=[col for col in table.keys()]    
    
     
    df=pd.DataFrame.from_records(rows, columns=header)
    
    # df = pd.DataFrame({'one': [-1, np.nan, 2.5],
    #                 'two': ['foo', 'bar', 'baz'],
    #                 'three': [True, False, True]},
    #                 index=list('abc'))
 

    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    

def fake_some_data_db(table_name,table,num_rows,db_conn):
    import pandas as pd
    #make row for each 
    rows=[]
    for _ in range(num_rows):
        row=[]
        for col in table.keys():
            data=table[col]()
            row.append(data)
        rows.append(row)
    header=[col for col in table.keys()]    
    
     
    pd=pd.DataFrame.from_records(rows, columns=header)
    engine=db_conn.connect_SqlAlchemy()
      
    pd.to_sql(table_name,engine,if_exists='append',index=False,schema=db_conn.schema)
    

def get_table_column_types(db, table_name, trg_schema=None):

        import sqlalchemy, pprint
        if trg_schema is None:
            schema = db.schema
        else:
            schema = trg_schema
        con  = db.connect_SqlAlchemy()
        schema_meta = sqlalchemy.MetaData(bind=con, 
                    reflect=True, schema=schema)
        table = sqlalchemy.Table(table_name, schema_meta, schema=schema, autoload=True, autoload_with=con)
        cols={}
        for col in table.columns:
            col_length=None
            try:
                
                col_length=col.type.length
            except:
                pass
            str_type=str(col.type.python_type) 
            str_type=str_type.replace('>','')
            str_type=str_type.replace("<",'')
            str_type=str_type.replace("type",'')
            str_type=str_type.replace("'",'').strip()
            str_len=(','+str(col_length)) if col_length is not None else ''
            cols[str(col).split('.')[-1]]=str_type+str_len
        return cols       
        
def fake_some_data_csv(file_path,table,num_rows):
    
    #make row for each 
    rows=[]
    for _ in range(num_rows):
        row=[]
        for col in table.keys():
            data=table[col]()
            row.append(data)
        rows.append(row)
    header=[col.encode() for col in table.keys()]    
    import csv
     
    with open(file_path,'w') as f:
        print("writing file: ",os.path.abspath(file_path))
        wr = csv.writer(f)
        wr.writerow(header)
        wr.writerows(rows)
 
def main(yamlfile=None):
    # process_list = []
    args = parse_cli_args()
    # multi process here for now
    # process_yaml(args.yaml, args.log_level)
    yaml_file = None
    if not yamlfile is None:
        yaml_file=os.path.abspath(yamlfile)
    else:
        yaml_file = os.path.abspath(args.yaml)
    
    
    yaml_data = yaml.full_load(open(yaml_file))
    logging.info('Read YAML file: \n\t\t{}'.format(yaml_file))
    set_log_level(args.log_level)
    yaml_dict , db_conn= pre_process_yaml(yaml_file)
    if args.generate_yaml is not None:
        generate_yaml_from_db(db_conn,args.generate_yaml,yaml_data)
    else:
        tables=map_fake_functions('Tables',yaml_dict)
        for table in tables.keys():
            
            t=tables[table]
            if t is not None:
                if args.output=='CSV':
                    fake_some_data_csv(table+'.csv',t,int(args.num_rows))
                elif args.output=='PARQUET':
                    fake_some_data_parquet(table+'.parquet',t,int(args.num_rows))
                elif args.output=='DB':
                    fake_some_data_db(table,t,int(args.num_rows),db_conn)
                else:
                    print("unknow output so skipping table: {}".format(table))
if __name__ == '__main__':
    main()
    
 