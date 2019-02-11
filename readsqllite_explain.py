import sqlite3
from sqlite3 import Error
 
 
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
        :param db_file: database file
        :return: Connection object or None
    """
    try:
        conn1 = sqlite3.connect(db_file)
        return conn1
    except Error as e:
        print(e)
 
    return None
 
 
def select_all_rows(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM Users")
 
    rows = cur.fetchall()
 
    for row in rows:
        print(row)
 
 
def select_task_by_title(conn, title):
    """
    Query database by title
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM Users WHERE Title=?", (title,))
 
    rows = cur.fetchall()
 
    for row in rows:
        print(row)
 
def explain_row(conn, rowno):
    """
    Query database by rowno and EXPLAIN QUERY PLAN
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute("EXPLAIN QUERY PLAN SELECT * FROM Users WHERE UserID=?", (rowno,))
    
    rows = cur.fetchall()
 
    for row in rows:
        print(row)

def Tablenameinfo(conn, table_name):
    """
    Query database by Tablename and get Table_Info
    :return:
    """
    print_col = True
    
    cur = conn.cursor()
    cur.execute('PRAGMA TABLE_INFO({})'.format(table_name))
 
    rows = cur.fetchall()
 
    if print_col:
        for row in rows:
            print(row)


def main():
    database = "C:\\spark\\CaliforniaHousing\\Py-DS-ML-Bootcamp-master\\Refactored_Py_DS_ML_Bootcamp-master\\04-Pandas-Exercises\\demo.db"
    conn = create_connection(database)
    table_name = 'Users'
    row_no =2

    with conn:
        print("1. Query task by priority:")
        select_task_by_title(conn,'ms')

        print("2. Query all Users")
        select_all_rows(conn)
        
        print("3. Explain Query:")
        explain_row(conn, row_no)

        print("4. Table Info:")
        Tablenameinfo(conn,table_name)

if __name__ == '__main__':
    main()