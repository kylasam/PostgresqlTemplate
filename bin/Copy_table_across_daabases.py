import psycopg2
from configparser import ConfigParser
from collections import namedtuple
import pandas as pd
import sys,time
from datetime import datetime
import logging

#LOG_FILE_NAME=datetime.now().strftime('ETL_process_%Y%m%d%H%M%S.log')
LOG_FILE_NAME=datetime.now().strftime('../log/copy_table_across_databases_%Y%m%d.log')
pd.set_option('display.max_columns', None)
logging.basicConfig(filename=LOG_FILE_NAME,
                    format='%(levelname)s::%(asctime)s.%(msecs)03d  From Module = ":%(funcName)s:" Message=> %(message)s.',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def get_config_details(config_path):
    config=ConfigParser()
    config.read(config_path)
    conf_evaluation={'host' : config.get('database','host'),
          'user' : config.get('database','user'),
          'password' : config.get('database','password'),
          'database_source' : config.get('database_source','database_source'),
          'database_target': config.get ('database_target', 'database_target'),
          'source_file' : config.get('workspaces','source_file'),
          'delta_file': config.get ('workspaces', 'delta_file'),
          'processing_data': config.get ('workspaces', 'processing_data'),
          'target_data': config.get ('workspaces', 'target_data'),
          'Master_data_table' : config.get('database','master_data_table'),
          'Delta_data_table' : config.get('database','Delta_data_table'),
          'Master_data_table_source_columns' : config.get('database','Master_data_table_source_columns'),
          'Master_table_creation_columns': config.get ('database', 'Master_table_creation_columns'),
          'Delta_data_table_source_columns': config.get ('database', 'Delta_data_table_source_columns'),
          'Delta_table_creation_columns': config.get ('database', 'Delta_table_creation_columns'),
          'Key_column' : config.get ('database', 'Key_column'),
          'update_col_values' : config.get ('database', 'update_col_values'),
          'update_grp_col' : config.get ('database', 'update_grp_col'),
          'report_file' : config.get ('workspaces', 'report_file')
          }
    conf_contents = namedtuple("Config", conf_evaluation.keys ()) (*conf_evaluation.values ())
    print(conf_contents)
    return conf_contents

def db_connect_postgresql(hostname,username,database,password):
    logging.info('Connecting to the PostgreSQL database...')
    try:
        conn = psycopg2.connect (host=hostname, database=database, user=username, password=password)
        cur = conn.cursor ()
        logging.info ("Postgresql Connection established Successfully...")
        return cur,conn
    except:
        logging.error("Postgresql Connection FAILED!!!!")
        return False


def db_table_setup (table_name,db_cur,db_connection,table_columns):
    logging.warning("Dropping the table " + '"' + table_name + '"' + " will happen in this phase.")
    QUERY_DROP_TABLE = """DROP TABLE IF EXISTS %s""" % (table_name)
    db_cur.execute (QUERY_DROP_TABLE)
    logging.info("Query for Dropping table " + '"' + table_name + '"' + " has EXECUTED SUCESSFULLY!!. Table creation process Initiated ")
    QUERY_CREATE_TABLE="""CREATE TABLE %s  (%s)""" %(table_name,table_columns)
    db_cur.execute(QUERY_CREATE_TABLE)
    logging.info ("Table " + '"' + table_name + '"' + " CREATED SUCCESSFULLY!!! ")
    db_connection.commit ()


def db_extract_databse(db_cur,db_connection,Master_table,report_file_dtls):
    logging.info("Process has kick started for Extract the Master data process....")
    try:
        EXTRACT_QUERY="COPY %s TO '%s' DELIMITER ',' CSV HEADER" %(Master_table,report_file_dtls)
        db_cur.execute (EXTRACT_QUERY)
        db_connection.commit ()
        logging.info ("Table " + '"' + Master_table + '"' + " has been EXPORTED SUCCESSFULLY into a file in the path " + '"' + report_file_dtls + '"' + " without any errors!!!")
    except (Exception, psycopg2.DatabaseError) as error:
         logging.error ("Target Table " + '"' + Master_table + '"' + " EXTRACTION FAILED!!!")
         print (error)
         exit(999)


def main():
    # Get configuration values from external file
    #conf = args.config
    conf ="C:\\Users\\user\\PycharmProjects\\CodeChallenge\\script_copy_tables_config.ini"
    config = get_config_details(conf)
    logging.info("Started the Database copy process....")

    #Evaluate the Connection details for Postgresql
    # Login to Source database and extract the data into CSV
    initialze_db,initialze_conn=db_connect_postgresql(config.host,config.user,config.database_source,config.password)

    #perform Downloading the report into a file and Email(If value passed from user console)
    execute_reporting=db_extract_databse(initialze_db,initialze_conn,config.Master_data_table,config.report_file)

    #Evaluate the Connection details for Postgresql
    # Login to Source database and extract the data into CSV
    initialze_db,initialze_conn=db_connect_postgresql(config.host,config.user,config.database_target,config.password)

    # Create the Table if not exists; Drop if exists
    db_table_setup(config.Delta_data_table, initialze_db,initialze_conn,config.Delta_table_creation_columns)

    #Load the Excel file into postgresql Tables
    Delta_load=load_csv_to_postgres_db("Delta Table Load",config.Delta_data_table,config.delta_file,initialze_db,initialze_conn,config.Delta_data_table_source_columns,config.Delta_data_table_source_columns)




if __name__ == '__main__':
 RETVAL = main()
 sys.exit(RETVAL)