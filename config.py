import os
# Archive file path

ARCHIVE_FILE_PATH = 'C:\SparkCourse'

# Set file name

FILE_NAME = '\wechat_test.7z'

# Set path to file

FILE_PATH = os.getcwd()

# Spark wh dir

WH_DIR = 'spark.sql.warehouse.dir'

# Spark temp dir path

TEMP_DIR = 'file:///C:/temp'

# ClickAnalysis consolidated directory

CLICK_ANALYSIS_DIR = os.getcwd() + '\Analysis\ClickAnalysisDir'

# BizAnalysis consolidated directory

BIZ_ANALYSIS_DIR = os.getcwd() + '\Analysis\BizAnalysisDir\BizAnalysisOutcome'

# Location to extracted clicks file

CLICKS_FILE_TO_PROC = 'file:///C:/SparkCourse/wechat_test/clicks'

# Location to extracted biz file

BIZ_FILE_TO_PROC = 'file:///C:/SparkCourse/wechat_test/biz'

# Path to main directory

MAIN_DIR_PATH = os.getcwd() + '\Analysis'

# Database configuration

DATABASE_CONFIG = {
    'DB_USER': 'postgres',
    'DB_PWD': '',
    'DB_HOST': 'localhost',
    'DB_PORT': 5432,
    'DB_NAME': 'postgres'
}

# Terminate all sessions except yours

TERMINATE_SESSIONS = "SELECT  pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = '%s';"

# Count databases

COUNT_DATABASES = "SELECT COUNT(1) FROM pg_database WHERE datname = '%s' ;"

# Drop db statement

DROP_DB = "DROP DATABASE %s ;"

# Create db statement

CREATE_DB = "CREATE DATABASE %s  ;"



# SQL statements to create tables

CREATE_TABLES_STMT = (
        """
        CREATE TABLE IF NOT EXISTS clicks (
            id INT PRIMARY KEY NOT NULL,
            URL VARCHAR(512) NOT NULL,
            like_number INT,
            read_number INT,
            timestamp DATE,
            title VARCHAR(256)
        )
        """,
        """ CREATE TABLE IF NOT EXISTS biz (
                biz_id VARCHAR(128) PRIMARY KEY NOT NULL,
                biz_desc VARCHAR(512),
                biz_name VARCHAR(256)
                )
        """
)
# Name of the new database

DB_NAME = 'http_analyzer'

# Insert into biz

INSERT_BIZ = "INSERT INTO title(biz_id, biz_desc, biz_name) VALUES (%s, %s, %s);"

# Insert into clicks

INSERT_CLICKS = "INSERT INTO title(id, URL, like_number, read_number, timestamp, title) VALUES (%s, %s, %s, %s, %s, %s);"


