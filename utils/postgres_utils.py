import psycopg2
import json, csv, sys

# Connect to a postgres DB
#
def connect_to_db(host,dbname,user,password):
	conn_str = "host={0}, dbname={1} user={2} password={3}".format(host,dbname,user,password)
	conn = psycopg2.connect(conn_str)
	return conn
	

def csv_to_table(conn,tbl_name,csv_file):
	cur = conn.cursor()

	with open(csv_file) as file:
		dr = list(csv.DictReader(file))
		col_names = str(tuple(dr[0].keys())) # Get column names from file
		num_cols = len(tuple(dr[0].keys())) # Get number of columns
		values_txt = str("%s,"*num_cols)[:-1] # For execute method
		to_db = [tuple(i.values()) for i in dr]
		i=0
		for row in to_db:
			cur.execute("INSERT INTO {0} VALUES ({1});".format(tbl_name,values_txt), row)
			i=i+1
			print("Row {0}".format(i))
	conn.commit()
	conn.close()
	