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
lg.basicConfig()
logging = lg.getLogger('fake-me-some')
 

def printhello():
    print("------hellow")

def create_yaml(file_path):
    text="""db:
    connection: 
        db: postgres
        host: pgdb
        port: 5432
        type: 'POSTGRES'
        schema: "test"
        userid: 'docker'
        password_envir_var: PGPASSWORD\n"""
    with open(file_path,'w') as f:
        f.write(text)

def generate_yaml_from_db_suggest(db_conn, file_fqn, yaml_data):
    faker_list = []
    faker_file = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), "provider.yml")
    with open(faker_file, "r") as f:
        faker_list = yaml.safe_load(f)
   

    fqn = os.path.abspath(file_fqn)
    table_list = db_conn.get_all_tables()
    tbl = {}

    for t in table_list:
        if t.startswith(db_conn.schema+'.'):

            cols = match_name_to_type(db_conn, t, None, faker_list)

            tbl[str(t).split(".")[-1]] = cols
    tables = {"Tables": tbl}

    if os.path.isfile(fqn):
        print("File Already Exists Merging Updates")
        merge_dict_file(tables, fqn, yaml_data)
    else:
        with open(fqn, 'w') as outfile:
            dump_yaml_to_file(tables, outfile)
        merge_dict_file(tables, fqn, yaml_data)

def generate_yaml(yamlfile=None):
    db_conn_dict= {"db":{}}
    conn_dict=db_conn_dict["db"]["connection"]={}
    yaml_text="""db:
    connection: 
        db: postgres
        host: pgdb
        port: 5432
        type: 'POSTGRES'
        schema: "test"
        userid: 'docker'
        password_envir_var: PGPASSWORD """
    try:

        with open(yamlfile, 'w') as f:
            print("writing file: ", os.path.abspath(yamlfile))
            f.write("abc")

    except Exception as e:
        logging.error(e)


def match_name_to_type(db, table_name, trg_schema=None, faker_list=[]):
    from Levenshtein import ratio

    import sqlalchemy

    if trg_schema is None:
        schema = db.schema
    else:
        schema = trg_schema
    con = db.connect_SqlAlchemy()
    schema_meta = sqlalchemy.MetaData(bind=con,
                                      schema=schema)
    schema_meta.reflect()
    table = sqlalchemy.Table(table_name.split(
        '.')[-1], schema_meta, schema=schema, autoload=True, autoload_with=con)
    cols = {}

    for col in table.columns:
        closes_distance = 0.0
        match_name = None
        try:  # if string , varchar text..etc
 
            for provider in faker_list:
                # print(type(provider),provider)
                for fake in faker_list[provider]:

                    r = ratio(fake, str(col).split('.')[-1])

                    r1 = ratio(provider.split(".")
                               [-1]+"_"+fake, str(col).split('.')[-1])
                    if r1 > r:
                        r = r1
                    if r > closes_distance:
                        closes_distance = r
                        match_name = provider+"."+fake

                if closes_distance == 1:
                    break
        except Exception as e:
            if not str(col.type).upper() in ['BIGINT', 'INT', 'SMALLINT', 'INTEGER']:
                print(" Number field found ", col.type, col)
                print("\t\t", e)
            match_name = col.type

        cols[str(col).split('.')[-1]] = str(match_name).split('(')[0]

    return cols


def generate_yaml_from_db(db_conn, file_fqn, yaml_data):

    fqn = os.path.abspath(file_fqn)
    table_list = db_conn.get_all_tables()
    tbl = {}

    i = 0
    for t in table_list:

        if t.startswith(db_conn.schema+'.'):
            i += 1
            cols = get_table_column_types(db_conn, t)
            # order_dict=OrderedDict

            tbl[str(t).split(".")[-1]] = cols
    if i == 0:
        raise Exception(f"Not tables found in schema: {db_conn.schema}")
    tables = {"Tables": tbl}

    if not os.path.isfile(fqn):
        with open(fqn, 'w') as outfile:

            dump_yaml_to_file(tables, outfile)
    merge_dict_file(tables, fqn, yaml_data)

 
def dump_yaml_to_file(tables, outfile):

    def custom_dump_yaml(ordered_data, output_filename):
        #DB or Tables

        for i in ordered_data:

            output_filename.write(f"{i}:\n")
            # Tables names
            for j in ordered_data[i]:

                output_filename.write(f"  {j}:\n")
                for k in ordered_data[i][j]:

                    output_filename.write(
                        f"    {k}: {ordered_data[i][j][k]}\n")

    # dump_ordered_yaml(tables,outfile)
    custom_dump_yaml(tables, outfile)

# function to derive a function to generate data and return that function to be called later


def merge_dict_file(tables, file, yaml_data):

    root = 'Tables'
    has_root = False
    file_yaml = None
    db = yaml_data['db']
    with open(file, 'r') as stream:
        file_yaml = ordered_load(stream, yaml.SafeLoader)
        #file_yaml = yaml.safe_load((outfile))

        # usage example:
        ordered_load(stream, yaml.SafeLoader)
        try:
            if file_yaml.get(root, None):
                has_root = True
        except:
            has_root = False

    if not has_root:
        with open(file, 'a') as outfile:
            [dump_yaml_to_file(tables, outfile)]

    else:
        # loop through every tables found in DB
        for tbl in tables[root].keys():
            t = tables[root][tbl]
            # check to see if table is in yaml file
            # if not add everything in
            file_yaml_tbl = file_yaml[root].get(tbl, None)
            if file_yaml_tbl is None:
                print("addding to yaml ", tbl)
                file_yaml[root][tbl] = t

            else:
                # since table exist loop through each column
                for col in t.keys():
                    # if column doesn't exist in yaml add the column
                    if file_yaml[root][tbl].get(col, None) is None:
                        file_yaml[root][tbl][col] = t[col]

        if file_yaml.get('db', None) is None:
            file_yaml['db'] = db
        with open(file, 'w') as outfile:

            dump_yaml_to_file(file_yaml, outfile)


def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    class OrderedLoader(Loader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)

def get_table_column_types(db: postgres.DB, table_name, trg_schema=None):

    import sqlalchemy

    if trg_schema is None:
        schema = db.schema
    else:
        schema = trg_schema
    con = db.connect_SqlAlchemy()
    schema_meta = sqlalchemy.MetaData(bind=con,
                                      schema=schema)
    schema_meta.reflect()
    logging.info(" Current Table: {}".format(table_name))
    table = sqlalchemy.Table(table_name.split(
        '.')[-1], schema_meta, schema=schema, autoload=True, autoload_with=con)
    cols = OrderedDict()
    x, y = db.query(f"select * from {table_name} limit 1")

    ordered_column_list = [col for col in y]
    # print(ordered_column_list)

    for col in table.columns:
        col_length = None
        try:

            col_length = col.type.length

        except:

            pass
        str_type = str(col.type)
        order = 0
        found = False
        for i, ee in enumerate(ordered_column_list):

            if ee.name == col.name:
                order = i+1

        if i == 0:
            raise Exception("column not found")

        cols[order] = [str(col).split('.')[-1], str_type]
    cols2 = OrderedDict()

    for i, column in enumerate(cols):

        cols2[cols[column][0]] = cols[column][1]

    return (cols2)

 