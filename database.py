import psycopg2
import psycopg2.extras as extras
import sys
import pandas as pd
from dateutil.tz import tzlocal
import datetime

TIME_ZONE = tzlocal()

param_dic = {
    "host"      : "database-XXX.XXX.XXX.amazonaws.com",
    "database"  : "XXXX",
    "user"      : "XXXX",
    "password"  : "XXXX"
}


def connect(params_dic=None):
    """ Connect to the AWS RDS PostgreSQL database server """

    global param_dic

    if params_dic is None:
        params_dic = param_dic

    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

def create_table(conn, table_name):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS {0} (ID INTEGER, COL_1 INTEGER, COL_2 TEXT, COL_3 INTEGER, COL_4 BOOLEAN, COL_5 TEXT, WRITE_TIME TIMESTAMPTZ);".format(table_name))
    conn.commit()
    cur.close()

def create_log_table(conn, table_name):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS {0} (RUN_TIME TIMESTAMPTZ, SOURCE_COUNT INTEGER, DB_COUNT INTEGER, DELETED_ROWS INTEGER, RESYNCED_ROWS INTEGER, UPDATED_DB_COUNT INTEGER, SYNC_CHECK BOOLEAN);".format(table_name))
    conn.commit()
    cur.close()

def insert_log(conn, table_name, source, db_previous, deleted, resynced, updated, check):

    query = "INSERT INTO %s(RUN_TIME, SOURCE_COUNT, DB_COUNT, DELETED_ROWS, RESYNCED_ROWS, UPDATED_DB_COUNT, SYNC_CHECK) VALUES(" % table_name + "%s,%s,%s,%s,%s,%s,%s)"
    cursor = conn.cursor()

    try:
        cursor.execute(query, (datetime.datetime.now(tzlocal()), source, db_previous, deleted, resynced, updated, check))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("log inserted... ")
    cursor.close()

def execute_batch(conn, df, table, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(" % (table,cols) + "%s,"*(len(df.columns)-1) + "%s)"
    cursor = conn.cursor()
    try:
        extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_batch() done")
    cursor.close()


def delete_before_date(conn, table, date):
    query = "DELETE FROM %s WHERE " % table + "addedat > %s"
    cursor = conn.cursor()

    try:
        cursor.execute(query, (date,))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("delition done, rows deleted = ", cursor.rowcount)
    cursor.close()

    return cursor.rowcount

def resync(conn, df, table):
    query = "select id from %s " % table
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        ids = cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("ids extraction done")
    cursor.close()

    #list of present ids in db
    ids = list(pd.DataFrame(ids)[0].astype('str'))
    print("Total rows =", len(ids))

    #list of ids not present in data
    ids_diff = list(set(df['vid']) - set(ids))
    print("Rows difference =", len(ids_diff))

    #getting df of only non existant entries
    to_add = df[df['vid'].isin(ids_diff)]
    print("Rows to be added =", len(to_add))

    #insert
    execute_batch(conn, to_add, table)

    return len(to_add)

def row_count(conn, table):
    query = "select id from %s " % table
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        ids = cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("Total rows = ", len(ids))
    cursor.close()

    return len(ids)

# fetch data from source and bring in pandas data frame format
def fetch_all_data():
    pass

def update(table_name, sync_days=0):
    resync_date = datetime.datetime.now(TIME_ZONE) - datetime.timedelta(days=sync_days)
    # resync_date = resync_date.strftime("%Y-%m-%d")
    print("Resync date =", resync_date)

    #fetch all the data to resync
    print("fetching all the data...")
    df = fetch_all_data()
    source = len(df)

    conn = connect()

    #keep record of previos count
    db_previous = row_count(conn, table_name)

    create_table(conn, table_name)

    #delete data for given sync days
    print('deleting...')
    deleted = delete_before_date(conn, table_name, resync_date)

    #resync to get updated data
    print('resyncing...')
    prep_df = prep_data(df)
    resynced = resync(conn, prep_df, table_name)

    #updated db status check
    updated = row_count(conn, table_name)
    check = (updated == source)
    print("Count same =", check)

    return source, db_previous, deleted, resynced, updated, check
