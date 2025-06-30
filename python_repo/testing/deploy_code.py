
import oracledb
import sys
import os
import platform
import pdb
import re
import functools
import math
print (sys.path)
from oraclemonitor import OracleMonitor
from collections import Counter
import logging
import traceback

# Get the logger for the main module
logger = logging.getLogger(__name__)

# Configure logging level
logger.setLevel(logging.DEBUG)

def handle_exception(exc_type, exc_value, exc_traceback): # Define the global exception handler function
    exc_info = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    logger.error("Uncaught Exception: %s", exc_info)

sys.excepthook = handle_exception  #Set the global exception handler

# create file handler which logs debug messages
fh = logging.FileHandler('tacticalTablespaces.log')
fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

logger.propagate = False

logger.info(f"Starting DeployCode.py")

if sys.maxsize > 2147483647:
  logger.info(f"Running in 64-bit mode.")
else:

  logger.info(f"Not running in 64-bit mode.")

d = None  # default suitable for Linux
if platform.system() == "Windows":
    #d = r"E:\Oracle\client\product\instant"
    d =r"C:\oracle\product\instantclient_19_20"

# Initialize the Oracle client
oracledb.init_oracle_client(lib_dir=d)


class DeployCode(OracleMonitor):
    QUERY_TIMEOUT = 30
    LOGIN_TIMEOUT = 30

    def __init__(self):
        super().__init__()
        self.min_percent = 3.0
        self.tns=""
        self.dns=""
        self.host_val=""
        self.port_val=0
        self.service_val=""
        self.user_val=""
        self.pword_val=""
        self.db_name=""
        self.connection= None
        self.additional_space_needed_mb= None
        self.remaining_space_needed= None
        
    def process_database(self, db):
        logging.info(f"NOTE: Processing {db.get_name()}, tns={db.get_tns()} ...")
        #print("Service name is :" , service_val)  
        self.db_name=db.get_name()
        dns=db.get_name()
        self.user_val=db.get_username()
        self.pword_val=db.get_password()
        
        try:
            #self.connection = oracledb.connect(user=self.user_val, password=self.pword_val, host=self.host_val, service_name=self.service_val, port=self.port_val)
            #The following Standalone connection works but will fail if i tried to add the timeout parameter
            #connection = oracledb.connect(user=self.user_val, password=self.pword_val, dsn=db.get_name())
            
            #In order to be able to implement connection TIME_OUT i had to to use connection pooling as this is not avaiable with Standalone connections
            pool = oracledb.create_pool(user=self.user_val, password=self.pword_val, dsn=db.get_name(),min=1, max=5, increment=1, timeout=self.LOGIN_TIMEOUT)
            self.connection = pool.acquire()
        except oracledb.DatabaseError as error:
            logger.exception(f"Database connection error encountered:, {error}")
            db_available = 0 
            #print(f"CONN ERROR: {str(error)}")
            #print("Conn Details are:" , self.host_val, self.port_val,self.service_val)
            logger.info(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
            return   
        cursor = self.connection.cursor()
        cursor.arraysize = 1000
        db_available = 1 
        logger.info(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")

        self.config_hp_diag(db)

    def config_hp_diag(self, db):
        # SQL to check if the function and types already exist
        sql_check = """
            SELECT object_name
            FROM dba_objects
            WHERE owner = 'HP_DIAG'
            AND object_name IN ('READ_FILE', 'FILE_LINE_ROW', 'FILE_LINE_TABLE')
            AND object_type IN ('FUNCTION', 'TYPE')
            UNION ALL
            SELECT directory_name object_name from dba_directories
            where directory_name='TEMP'
        """

        # SQL to create object type
        create_type_row_sql = """
            CREATE OR REPLACE TYPE HP_DIAG.file_line_row AS OBJECT (
                line_id NUMBER,
                line_content VARCHAR2(4000)
            )
        """

        # SQL to create table type
        create_type_table_sql = """
            CREATE OR REPLACE TYPE HP_DIAG.file_line_table AS TABLE OF HP_DIAG.file_line_row
        """

        # SQL to create table directory object TEMP
        create_dir_obj_sql = """
            CREATE DIRECTORY TEMP AS '/tmp'
        """

        #Grant permissions
        grant_perms_sql = """
            GRANT READ, WRITE ON DIRECTORY TEMP to HP_DIAG
        """

        # SQL to create function
        create_function_sql = """
            CREATE OR REPLACE FUNCTION HP_DIAG.read_file(p_directory VARCHAR2 DEFAULT 'TEMP', p_filename VARCHAR2 DEFAULT '/tmp/ora_arch_mon.out')
            RETURN HP_DIAG.file_line_table PIPELINED IS
                file_handle UTL_FILE.FILE_TYPE;
                line VARCHAR2(32767);
                line_id NUMBER := 1; -- Initialize line counter
            BEGIN
                file_handle := UTL_FILE.FOPEN(p_directory, p_filename, 'r');
                LOOP
                    BEGIN
                        UTL_FILE.GET_LINE(file_handle, line);
                        -- Pipe the line back as a row
                        PIPE ROW(HP_DIAG.file_line_row(line_id, line));
                        line_id := line_id + 1; -- Increment line counter
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            EXIT; -- Exit the loop when no more data is found
                    END;
                END LOOP;
                UTL_FILE.FCLOSE(file_handle);
                RETURN;
            EXCEPTION
                WHEN OTHERS THEN
                    IF UTL_FILE.IS_OPEN(file_handle) THEN
                        UTL_FILE.FCLOSE(file_handle);
                    END IF;
                    RAISE;
            END;
        """

        try:
            cursor = self.connection.cursor()
            cursor.arraysize = 1000
            cursor.execute(sql_check)
            existing_objects = {row[0] for row in cursor.fetchall()}  # Use a set for efficient look-up

            # Create types if they do not exist
            if 'FILE_LINE_ROW' not in existing_objects:
                logger.info(f"Creating object for monitoring archive log filesystem...")
                cursor.execute(create_type_row_sql)
                logger.info(f"Type HP_DIAG.FILE_LINE_ROW created successfully.")
            if 'FILE_LINE_TABLE' not in existing_objects:
                logger.info(f"Creating object for monitoring archive log filesystem...")
                cursor.execute(create_type_table_sql)
                logger.info(f"Type HP_DIAG.FILE_LINE_TABLE created successfully.")

            # Check if function needs to be created
            if 'READ_FILE' not in existing_objects:
                logger.info(f"Creating object for monitoring archive log filesystem...")
                cursor.execute(create_function_sql)
                logger.info(f"Function HP_DIAG.READ_FILE created successfully.")
            
            # check if TEMP DIRECTORY needs creating
            if 'TEMP' not in existing_objects:
                logger.info(f"Creating database directory for accessing file...")
                cursor.execute(create_dir_obj_sql)
                logger.info(f"Directory TEMP created successfully.")
                logger.info(f"Granting read/write access on database directory TEMP to HP_DIAG...")
                cursor.execute(grant_perms_sql)
                logger.info(f"Permissions on TEMP directory granted successfully to HP_DIAG.")
                

        except Exception as e:
            logger.exception(f"Error in executing SQL or processing results: {str(e)}")
        finally:
            cursor.close()  # Ensure the cursor is closed after operation

    def check_args(self, args):
        if args is not None:
            if len(args) > 0:
                try:
                    #pcent_input = args[3]
                    #print("Printing raw value for -p: " , pcent_input)
                    self.min_percent = float(args[1])
                #except ValueError:
                except Exception as e:
                    #print("Invalid argument: Minimum free percent must be a valid floating-point number.")
                    logger.exception(f"Floating error is :, {e}")
                    return False
        return True  # Return True when parsing is successful

    def run(self):
        logger.info(f"NOTE: QUERY TIMEOUT      = {self.QUERY_TIMEOUT} seconds")
        logger.info(f"NOTE: CONNECTION TIMEOUT = {self.LOGIN_TIMEOUT} seconds")
        args = sys.argv[1:]  # Get command-line arguments here
        #print("The arguments:", args)
        if self.check_args(args):
            if self.populate():
                logger.info(f"DEBUG: Number of databases being checked:, {len(self.databases)}")
                for db in self.databases:  # Loop through the databases       
                    self.process_database(db)       
            else:
                logger.info(f"Failed to extract the command line aruguments into variables")

if __name__ == "__main__":
    oracle_monitor = DeployCode()
    #print("Entering run() method for main class.")
    oracle_monitor.run()