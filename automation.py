# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2
# Connect to MySQL
connection = mysql.connector.connect(user='root', password='MTY0My1zYXRoaXNo',host='127.0.0.1',database='sales')
# Connect to DB2 or PostgreSql
dsn_hostname = '127.0.0.1'
dsn_user='postgres'        # e.g. "abc12345"
dsn_pwd ='MTEzODktc2F0aGlz'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="postgres"           # i.e. "BLUDB"


# create connection

conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)
# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.
cursor = conn.cursor()
def get_last_rowid():
    SQL = ('SELECT rowid FROM sales_data ORDER BY rowid DESC LIMIT 1')
    cursor.execute(SQL)
    lastrow = cursor.fetchone()
    return lastrow[0]


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    connection = mysql.connector.connect(user='root', password='MTY0My1zYXRoaXNo',host='127.0.0.1',database='sales')	
    cursor = connection.cursor()
    SQL = ('SELECT * FROM sales_data')
    cursor.execute(SQL)
    newarray = []
    for row in cursor.fetchall():
        if row[0] > rowid:
            newarray.append(row)
    return newarray
new_records = get_latest_records(last_row_id)

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    cursor = conn.cursor()
    for row in records:
        SQL = ('INSERT INTO sales_data(rowid, product_id, customer_id, quantity) values (%s,%s,%s,%s)')
        cursor.execute(SQL, row)
        conn.commit()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connection.close()
# disconnect from DB2 or PostgreSql data warehouse 
conn.close()
# End of program