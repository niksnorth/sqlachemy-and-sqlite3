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
    Query tasks by priority
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
    Query tasks by priority
    :param conn: the Connection object
    :param priority:
    :return:
    """
    cur = conn.cursor()
    cur.execute("EXPLAIN QUERY PLAN SELECT * FROM Users WHERE UserID=?", (rowno,))
 
    rows = cur.fetchall()
 
    for row in rows:
        print(row)


def main():
	database = "C:\\Users\\nksingh\\Documents\\ELY\\PythonScripts\\Refactored_Py_DS_ML_Bootcamp-master\\07-Pandas-Built-in-Data-Viz\\demo.db"
	conn = create_connection(database)

	with conn:
		print("1. Query task by priority:")
		select_task_by_title(conn,2)
 
		print("2. Query all Users")
		select_all_rows(conn)

		print("3. Explain Query:")
		explain_row(conn,2)
		
if __name__ == '__main__':
    main()