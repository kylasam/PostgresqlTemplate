import psycopg2
from configparser import ConfigParser
from collections import namedtuple
import pandas as pd
import sys,time
from datetime import datetime
import logging

#LOG_FILE_NAME=datetime.now().strftime('ETL_process_%Y%m%d%H%M%S.log')
LOG_FILE_NAME=datetime.now().strftime('../log/ETL_process_%Y%m%d.log')
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
          'database' : config.get('database','database'),
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

def load_csv_to_postgres_db(type_of_load,table_name,source_file,db_cur,db_connection,Master_table_columns,delta_table_columns):
    CSV_COUNT = sum (1 for line in open (source_file)) - 1
    logging.info(" " + type_of_load + " Process Started")
    logging.info("Columns that will be load in this phase are "  + Master_table_columns + ".")
    logging.info ("In the source file " + source_file + " there were total of " + str (CSV_COUNT) + " lines present excluding Header")
    try:
        LOAD_MSTR_TABLE="COPY %s(%s) FROM '%s' DELIMITER ',' CSV HEADER"  %(table_name,Master_table_columns,source_file)
        db_cur.execute (LOAD_MSTR_TABLE)
        db_connection.commit ()
        # Estimate the Total no.of records loaded in the table successfully!!!
        db_cur.execute("SELECT COUNT(*) FROM %s limit 1" %(table_name))
        TOTAL_RECORDS_LOADED_IN_PSTGRESQL=db_cur.fetchone()
        if (CSV_COUNT != TOTAL_RECORDS_LOADED_IN_PSTGRESQL[0] ):
            logging.info ("Table " + '"' + table_name  + '"' +  " is being loaded with only " + str(TOTAL_RECORDS_LOADED_IN_PSTGRESQL[0]) + " records but in the file we had ." + CSV_COUNT + " records. Halting the process!!!" )
            exit(102)
        else:
            logging.info ("Table " + '"' + table_name  + '"' + " is being loaded with " + str(TOTAL_RECORDS_LOADED_IN_PSTGRESQL[0]) + " records Successfully.")
            logging.info ("Table Load for " + table_name + " COMPLETED SUCCESSFULLY!!!")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Table Load process failed with Database Error!")
        print (error)
        exit (109)

def db_perform_delata_load(db_cur,db_connection,Master_table,Delta_table,Master_table_columns,key_column,update_column_names,update_grp_col):
    logging.info("Initializing the process for the Data Load from " + '"' + Delta_table + '"' + " to the target table " + '"' + Master_table + '".' )
    try:
        logging.info ("Try block gets kicked for the Delta process into the table " + '"' + Master_table + '"' + " from the table " + '"' +  Delta_table + '".')
        try:
            INSERT_QUERY="INSERT INTO %s SELECT %s FROM %s where indicator='I'" %(Master_table,Master_table_columns,Delta_table)
            db_cur.execute (INSERT_QUERY)
            db_connection.commit()
            logging.info("Table " + '"' + Master_table + '"' + " has been INSERTED with NEW RECORDS SUCCESSFULLY" )
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("Insert process FAILED into the table " + '"' + Master_table + '"' + " for New records")
            print(error)
        logging.info ("Initializing the process for Drop records on" + '"' + Master_table + '"' + " from delta whose indicator is D")
        try:
            DELETE_QUERY="DELETE FROM %s WHERE %s IN (SELECT %s FROM %s where indicator='D')" %(Master_table,key_column,key_column,Delta_table)
            db_cur.execute (DELETE_QUERY)
            db_connection.commit ()
            logging.info ("Table " + '"' + Master_table + '"' + " has been DELETED with OLD RECORDS SUCCESSFULLY")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error ("Dropping Old records process FAILED into the table " + '"' + Master_table + '"' + " for New records")
            print (error)
        logging.info("Initializing the process for Update records in " + '"' + Master_table + '"' + " from delta whose indicator is U")
        try:
            UPDATE_QUERY="UPDATE %s c SET %s from (select %s from (select * ,rank() OVER (PARTITION BY %s order by %s desc) from %s where indicator='U') AS A where A.rank=1) tabl where c.%s=tabl.%s" %(Master_table,update_column_names,Master_table_columns,key_column,update_grp_col,Delta_table,key_column,key_column)
            db_cur.execute (UPDATE_QUERY)
            db_connection.commit ()
            logging.info ("Table " + '"' + Master_table + '"' + " has been UPDATED SUCCESSFULLY")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error ("Updating table records process FAILED for the table " + '"' + Master_table + '"' + " for Update Algoritm")
            print (error)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error ("Delta proces on the table " + '"' + Master_table + '"' + " FAILED WITH ERROR!!HALTING THE PROCESS!!!")
        print (error)
        exit(99)

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

def ad_hoc_process(db_cur,db_connection,Master_table):
    logging.info("Initializing the process for the Ad hoc process for the " + '"' + Master_table + '"' + " processing " + '"' +  '".' )
    try:
        ADHOC_QUERY="SELECT * FROM  %s " %(Master_table)
        db_cur.execute (ADHOC_QUERY)
        ADHOC_QUERY_RESULT=db_cur.fetchall()
        db_connection.commit()
        print(ADHOC_QUERY_RESULT)
        logging.info("Table " + '"' + Master_table + '"' + " has been EXECUTED SUCCESSFULLY" )
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Ad-hoc process FAILED for the  table " + '"' + Master_table + '"' + " processing!!!")
        print(error)


def main():
    # Get configuration values from external file
    #conf = args.config
    conf ="C:\\Users\\user\\PycharmProjects\\CodeChallenge\\config.ini"
    config = get_config_details(conf)
    logging.info("Started the ETL process....")

    #Evaluate the Connection details for Postgresql
    initialze_db,initialze_conn=db_connect_postgresql(config.host,config.user,config.database,config.password)

    # Create the Table if not exists; Drop if exists
    db_table_setup(config.Master_data_table,initialze_db,initialze_conn,config.Master_table_creation_columns)
    db_table_setup(config.Delta_data_table, initialze_db,initialze_conn,config.Delta_table_creation_columns)

    #Load the Excel file into postgresql Tables
    Master_load=load_csv_to_postgres_db("Master table Load",config.Master_data_table,config.source_file,initialze_db,initialze_conn,config.Master_data_table_source_columns,config.Delta_data_table_source_columns)
    Delta_load=load_csv_to_postgres_db("Delta Table Load",config.Delta_data_table,config.delta_file,initialze_db,initialze_conn,config.Delta_data_table_source_columns,config.Delta_data_table_source_columns)

    # Perform the SCD type 2 process
    Delta_processing=db_perform_delata_load(initialze_db,initialze_conn,config.Master_data_table,config.Delta_data_table,config.Master_data_table_source_columns,config.Key_column,config.update_col_values,config.update_grp_col)

    #Perform any Query Execution process as Ad-hoc Query Execution process
    ad_hoc_execution=ad_hoc_process(initialze_db,initialze_conn,config.Master_data_table)

    #perform Downloading the report into a file and Email(If value passed from user console)
    execute_reporting=db_extract_databse(initialze_db,initialze_conn,config.Master_data_table,config.report_file)


if __name__ == '__main__':
 RETVAL = main()
 sys.exit(RETVAL)
