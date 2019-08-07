import psycopg2
import config
import os
import gzip
import csv


def create_database(self):

    connection = None
    try:
        connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                      password=config.DATABASE_CONFIG['DB_PWD'],
                                      host=config.DATABASE_CONFIG['DB_HOST'],
                                      port=config.DATABASE_CONFIG['DB_PORT'],
                                      database=config.DATABASE_CONFIG['DB_NAME'])
        connection.autocommit = True
        print("Database connected successfully!")
        cur = connection.cursor()
        cur.execute(config.TERMINATE_SESSIONS % self.db_name)
        cnt_stmt = (config.COUNT_DATABASES % self.db_name)
        cur.execute(cnt_stmt)
        cnt_pointer = cur.fetchone()
        if cnt_pointer[0] == 1:
            cur.execute(config.DROP_DB % self.db_name)
            cur.execute(config.CREATE_DB % self.db_name)
        else:
            cur.execute(config.CREATE_DB % self.db_name)
        print(self.db_name + " database has been created.")
        return self.db_name
    except (Exception, psycopg2.Error) as error:
        print("Error while creating/dropping database: ", error)
    finally:
        if connection is not None:
            # Close connection
            connection.close()
            print('Database connection closed.')


def create_tables(self):
    connection = None
    commands = config.CREATE_TABLES_STMT
    try:
        # Establish connection
        connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                      password=config.DATABASE_CONFIG['DB_PWD'],
                                      host=config.DATABASE_CONFIG['DB_HOST'],
                                      port=config.DATABASE_CONFIG['DB_PORT'],
                                      database=self.db_name)
        connection.autocommit = True
        cur = connection.cursor()
        print("Trying to create tables...")
        for command in commands:
            cur.execute(command)
            print("Table has been created.")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating/dropping tables: ", error)
    finally:
        if connection is not None:
            # Close connection
            connection.close()
            print('Database connection closed.')


def insert_tsv_data(self):
    connection = None
    try:
        # Establish connection
        connection = psycopg2.connect(user=config.DATABASE_CONFIG['DB_USER'],
                                      password=config.DATABASE_CONFIG['DB_PWD'],
                                      host=config.DATABASE_CONFIG['DB_HOST'],
                                      port=config.DATABASE_CONFIG['DB_PORT'],
                                      database=self.db_name)
        connection.autocommit = True
        cur = connection.cursor()

        for file in os.listdir(config.BIZ_ANALYSIS_DIR):
            if file.startswith("part"):
                with open(file, encoding="utf8") as biz_file:
                    tsv_rows_reader = csv.reader(biz_file, delimiter='\t')
                    next(tsv_rows_reader)
                    print("Inserting rows...")
                    for row in tsv_rows_reader:
                        # Create a values row to insert
                        record_to_insert = (row[0], row[1], row[2])
                        # Execute statement and row
                        cur.execute(config.INSERT_BIZ, record_to_insert)
                continue
            else:
                continue

        for file in os.listdir(config.BIZ_ANALYSIS_DIR):
            if file.startswith("part"):
                with open(file, encoding="utf8") as click_file:
                    tsv_rows_reader = csv.reader(click_file, delimiter='\t')
                    next(tsv_rows_reader)
                    print("Inserting rows...")
                    for row in tsv_rows_reader:
                        # Create a values row to insert
                        record_to_insert = (row[0], row[1], row[2])
                        # Execute statement and row
                        cur.execute(config.INSERT_CLICKS, record_to_insert)
                continue
            else:
                continue

    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data into tables: ", error)
    finally:
        print("Inserting completed!")
        if connection is not None:
            # Close connection
            connection.close()
            print('Database connection closed.')