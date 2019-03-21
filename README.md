# fake-me-some
utility to fake some data for a database

#this will connect to your database and dump everytable and column 
python fake-me-some.py --yaml=config.yaml --generate_yaml=sample.yaml --log_level='INFO'

- go in and modify the sample.yaml to configure how you want to fake your data up.
- The config.yaml file has example
# this will dump 1000 rows into your Database
python fake-me-some.py --log_level='INFO' --output=DB --num_rows=1000 --yaml=sample.yaml
this will make a csv file for each table with 1000 rows
python fake-me-some.py --log_level='INFO' --output=CSV --num_rows=1000 --yaml=sample.yaml

this will make a parquet file for each table with 1000 rows
python fake-me-some.py --log_level='INFO' --output=CSV --num_rows=1000 --yaml=sample.yaml