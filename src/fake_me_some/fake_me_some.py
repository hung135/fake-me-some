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
    yaml_data = None
    with open(yaml_file,"r") as f:
        yaml_data = yaml.full_load((f))
    
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
                column_type=t[col]
                
        
                print("---------------",col,)
                print("-------xxx--------",col,column_type)
                if str(column_type).startswith('provider.'):
                    xx=fake_data(column_type)
                    column_type=xx
                elif str(column_type).startswith('NUMERIC'):
                    print("-------Numeric--------",col,column_type)
                    def rnd_float(start=0,end_max=sys.maxsize):
                        key_num = random.random()
                        return key_num
                    t[col]=rnd_float

                elif len(column_type.split(','))>1 and col.startswith('VARCHAR'):
                    
                    str_len=int(column_type.split(',')[1])
                    def rnd_str(int_len=str_len):
                        return random_string_generator(str_len,None)
                    t[col]=rnd_str
                    
                else:
                    print("-------Int--------",col,column_type)
                    def rnd_int(start=0,end_max=sys.maxsize):
                        key_num = random.SystemRandom()
                        return key_num.randint(0, 65045)
                            
                    t[col]=rnd_int

        print(tables)
    return tables
#leave what's already in the yaml file there and add in what's new
def merge_dict_file(tables,file,yaml_data):
    
    root='Tables'    
    has_root=False
    file_yaml=None
    db=yaml_data['db']
    with open(file, 'r') as outfile:
        file_yaml=yaml.full_load((outfile))
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
            tbl[str(t).split(".")[-1]]=cols
    tables={"Tables":tbl}
   
    
    
    if os.path.isfile(fqn):
        print("File Already Exists Merging Updates")
        merge_dict_file(tables,fqn,yaml_data)
    else:
        with open(fqn, 'w') as outfile:
            yaml.dump(tables, outfile, default_flow_style=False)
            
def generate_yaml_from_db_suggest(db_conn,file_fqn,yaml_data):
    faker_list=[]
    faker_file=os.path.join(os.path.dirname(os.path.abspath(__file__)),"provider.yml")
    with open(faker_file,"r") as f:
        faker_list=yaml.full_load(f)
        # for row in f:
        #     faker_list.append(row)
        # pass
     
    fqn=os.path.abspath(file_fqn)
    table_list=db_conn.get_all_tables()
    tbl={}
    
    for t in table_list:
        if t.startswith(db_conn.schema+'.'):
            
            cols=match_name_to_type(db_conn,t,None, faker_list)
            tbl[str(t).split(".")[-1]]=cols
    tables={"Tables":tbl}
   
    
    
    if os.path.isfile(fqn):
        print("File Already Exists Merging Updates")
        merge_dict_file(tables,fqn,yaml_data)
    else:
        with open(fqn, 'w') as outfile:
            yaml.dump(tables, outfile, default_flow_style=False)
        merge_dict_file(tables,fqn,yaml_data)
     

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
    logging.info("Faking Table: {} - {} Rows".format(table_name,num_rows))
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
    
def match_name_to_type(db,table_name, trg_schema=None,faker_list=[]):
    from Levenshtein import ratio 
      
    import sqlalchemy, pprint
    if trg_schema is None:
        schema = db.schema
    else:
        schema = trg_schema
    con  = db.connect_SqlAlchemy()
    schema_meta = sqlalchemy.MetaData(bind=con, 
                    schema=schema)    
    schema_meta.reflect()
     
    
    table = sqlalchemy.Table(table_name.split('.')[-1], schema_meta, schema=schema, autoload=True, autoload_with=con)
    cols={}
    
    for col in table.columns:
        closes_distance=0.0
        match_name=None
        try: # if string , varchar text..etc
            print("xxxxxxxxxxxxxxxxxxxxxxxxxxxx",col,col.type)
            col_length=col.type.length
            
            

            for provider in faker_list:
                #print(type(provider),provider)
                for fake in faker_list[provider]:  
                    #print(type(faker_list[]),"------------")
                    r=ratio(fake,str(col).split('.')[-1]) 
                    
                    r1=ratio(provider.split(".")[-1]+"_"+fake,str(col).split('.')[-1]) 
                    if r1>r:
                        r=r1
                    if r>closes_distance:
                        closes_distance=r
                        match_name=provider.split(".")[-1]+"."+fake
                
                #print(str(col), "-->",(lib),r)
                if closes_distance==1:
                    break 
        except Exception as e:
                print(" Number field found ",col.type,col)
                print("\t\t",e)
                match_name = col.type
        cols[str(col)]=str(match_name).split('(')[0]
        #print(col,"----->closes match--->",match_name,closes_distance)
    return cols   
    
def get_table_column_types(db, table_name, trg_schema=None):

        import sqlalchemy, pprint
        if trg_schema is None:
            schema = db.schema
        else:
            schema = trg_schema
        con  = db.connect_SqlAlchemy()
        schema_meta = sqlalchemy.MetaData(bind=con, 
                      schema=schema)    
        schema_meta.reflect()
        logging.info(" Current Table: {}".format(table_name))
        table = sqlalchemy.Table(table_name.split('.')[-1], schema_meta, schema=schema, autoload=True, autoload_with=con)
        cols={}
        for col in table.columns:
            col_length=None
            try:
                
                col_length=col.type.length
                
            except:
                
                pass
            str_type=str(col.type) 
            cols[str(col).split('.')[-1]]=str_type
        return cols       
        
def fake_some_data_csv(file_path,table,num_rows):
    
    #make row for each 
    rows=[]
    for _ in range(num_rows):
        row=[]
        for col in table.keys():
            print(col,"-------------------------------------....",table[col])
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
 
def main(yamlfile=None,p_output=None,p_generate=None,out_path=None):
    # process_list = []
    args = parse_cli_args()
    # multi process here for now
    # process_yaml(args.yaml, args.log_level)
    path=None
    if out_path is None:
        path=os.getcwd()
    else:
        path=os.path.abspath(out_path)
    yaml_file = None
    if not yamlfile is None:
        yaml_file=os.path.abspath(yamlfile)
    else:
        yaml_file = os.path.abspath(args.yaml)
    generate_yaml = p_generate or args.generate_yaml
    output = p_output or args.output
    yaml_data = None
    with open(yaml_file) as f:
        yaml_data = yaml.full_load((f))
    logging.info('Read YAML file: \n\t\t{}'.format(yaml_file))
    set_log_level(args.log_level)
    yaml_dict , db_conn= pre_process_yaml(yaml_file)
    if generate_yaml is not None:
        if output == 'SUGGEST':
            generate_yaml_from_db_suggest(db_conn,generate_yaml,yaml_data)
        if output != 'SUGGEST':
            generate_yaml_from_db(db_conn,generate_yaml,yaml_data)
    else:
        #map each column to a faker function
        tables=map_fake_functions('Tables',yaml_dict)
        for table in tables.keys():
            print("xxxxxxxxxxxxxxx",table)
            t=tables[table]
             
            if t is not None:
                if output=='CSV':
                    print("OUTPUT TO CSV:")
                    fake_some_data_csv(os.path.join(path,table+'.csv'),t,int(args.num_rows))
                elif output=='PARQUET':
                    fake_some_data_parquet(os.path.join(path,table+'.parquet'),t,int(args.num_rows))
                elif output=='DB':
                    print("OUTPUT TO DATABASE:")
                    fake_some_data_db(table,t,int(args.num_rows),db_conn)
                else:
                    print("unknow output so skipping table: {}".format(table))
if __name__ == '__main__':
    main()
    
 