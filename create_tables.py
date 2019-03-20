import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    try: 
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
         
        # create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
        
        # close connection to default database
        conn.close() 
        # connect to sparkify database
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e) 
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drop the tables
    :param cur: database cursor to execute the queries
    :param conn: conn to be used to commit and close the transaction
    :return: None
    """
    for query in drop_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Dropping table")
            print (e)
        


def create_tables(cur, conn):
    """
    Create the tables to be used in ETL process
    :param cur: database cursor to execute the queries
    :param conn: conn to be used to commit and close the transaction
    :return: None
    """
    for query in create_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: create_table_queries")
            print (e)


def main():
    """
    Main strating point of the file
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()