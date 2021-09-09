import database

if __name__ =='__main__':

    sync_days = 7
    source, db_previous, deleted, resynced, updated, check = database.update("TABLE_1", sync_days=sync_days)
    print(source, db_previous, deleted, resynced, updated, check)
    conn = database.connect()
    database.insert_log(conn, "TABLE_1_SYNC_LOGS", source, db_previous, deleted, resynced, updated, check)
