# fake-me-some
utility to fake some data for a database

This will connect to your database and dump everytable and column 
- python fake-me-some.py --yaml=config.yaml --generate_yaml=sample.yaml --log_level='INFO'

Go in and modify the sample.yaml to configure how you want to fake your data up.
The config.yaml file has example
Please refer to the faker_doc.txt file for all the types of data you can fake up.

This will dump 1000 rows into your Database
- python fake-me-some.py --log_level='INFO' --output=DB --num_rows=1000 --yaml=sample.yaml
This will make a csv file for each table with 1000 rows
- python fake-me-some.py --log_level='INFO' --output=CSV --num_rows=1000 --yaml=sample.yaml

This will make a parquet file for each table with 1000 rows
- python fake-me-some.py --log_level='INFO' --output=PARQUET --num_rows=1000 --yaml=sample.yaml
- 
PYTHON Environement Setup:
- virtualenv -p /usr/bin/python ~/.py27
- source ~/.py27/bin/activate
- pip install -r requirements.txt
- export MSPASSWORD=whateveryourpasswordis
- -MSPASSWORD is defined in your config.yaml file, you can make it whatever you want