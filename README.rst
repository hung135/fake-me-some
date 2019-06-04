============
fake-me-some
============


CLI to fake some data to CSV, Database, or Parquet


Description
===========

A longer description of your project goes here...
Go Here and Read and bash me or thank me
https://github.com/hung135/fake-me-some

Make a config file with this in it:
"""db:
    connection:
        appname: data_faker #used to tag dbconnection for visibility in db
        db: postgres
        host: pgdb
        port: 5432
        type: 'POSTGRES'
        schema: "test"
        userid: 'docker'
        password_envir_var: PGPASSWORD  """

Note
====

This project has been set up using PyScaffold 3.1. For details and usage
information on PyScaffold see https://pyscaffold.org/.
