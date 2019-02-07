import sqlite3
from sqlite3 import Error
import json, csv, sys


# Creates a SQLite database at the specified path
#	db_file: Complete file path of SQLite database file
#
def create_db(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()

		
# Imports a CSV file as a new SQLite DB Table
#	csv_file: Complete file path of csv file to import
#	tbl_name: Name of target table to create
#	db_file: Complete file path of SQLite database file
#
def create_table_from_csv(csv_file,tbl_name,db_file):
	con = sqlite3.connect(db_file);
	cur = con.cursor()
	
	# Check if table exists
	result = cur.execute("select name from sqlite_master where type='table' and name='{0}'".format(tbl_name))
	if len(list(result)) > 0:
		raise Exception('Table already exists, exiting code. Delete table or specify new table name.')

	with open(csv_file) as file:
		dr = list(csv.DictReader(file))
		col_names = str(tuple(dr[0].keys())) # Get column names from file
		num_cols = len(tuple(dr[0].keys())) # Get number of columns
		values_txt = str("?,"*num_cols)[:-1] # For executemany method
		cur.execute("CREATE TABLE {0} {1};".format(tbl_name,col_names))
		to_db = [tuple(i.values()) for i in dr]

	cur.executemany("INSERT INTO {0} {1} VALUES ({2});".format(tbl_name,col_names,values_txt), to_db)
	con.commit()
	con.close()


# Inserts data from a CSV file into a SQLite DB table
#	csv_file: Complete file path of csv file to import
#	tbl_name: Name of target table to create
#	db_file: Complete file path of SQLite database file
#
def insert_from_csv(csv_file,tbl_name,db_file):
	pass #To-do
	

# Converts SQL query output(i.e. from select statement) 
# into a pandas dataframe
#	sql: String of SQL code to run on the database
#	db_file: Complete file path of SQLite database file
#
def sql_to_df(sql,db_file):
	pass #To-do
	

# Runs SQL code on the database (for modifying data)
#	sql: String of SQL code to run on the database
#	db_file: Complete file path of SQLite database file
#
def run_sql(sql,db_file):
	pass #To-do
