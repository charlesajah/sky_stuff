
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

logger.info(f"Starting deploy_plan_prof_bkp_objects.py")

if sys.maxsize > 2147483647:
  logger.info(f"Running in 64-bit mode.")
else:

  logger.info(f"Not running in 64-bit mode.")

d = None  # default suitable for Linux
if platform.system() == "Windows":
    d = r"E:\Oracle\client\product\instant"
    #d =r"C:\oracle\product\instantclient_19_20"

# Initialize the Oracle client
oracledb.init_oracle_client(lib_dir=d)

class OracleTablespaceMonitorFreeSpaceNonFocus(OracleMonitor):
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
        self.space_allocations = {}  # Dictionary to keep track of allocations
        self.asm_space_allocations = {} # Dictionary to keep track of spaces into an ASM diskgroup
        self.tablespace_data = {}  # dictionary to store all tablespace OS Mount point data    
        self.generated_filenames = set()  # Global set to store generated filenames. Each element in a set is unique. I prefer it to Lists for this reason.
        self.MAX_FILE_SIZE_MB = 32767  # Maximum file size in MB
        
        
               

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
            skip_db = ['DMSO','INTSTGO','SMPGP5N','SMPUK5N','SMPIT5N','SMPDE5N','CAS011N','TCC011N']
            if db.get_name().upper() in skip_db:
                logger.info(f"Skipping database {db.get_name()} as it is in skip_db.")
                return
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
        
        #First.we create HP_DIAG objects required for monitoring archive_log filesystem
        self.config_hp_diag(db)

        # Check the archivelog filesystem
        #self.check_arch_filesystem(db)
            
        
        cursor.close()
        self.connection.close()
        pool.close()
        db_available = 1
        logger.info(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
        
 

    def config_hp_diag(self, db):
            
            # SQL to check if the tables already exist
            sql_check = """
                SELECT object_name
                FROM user_objects
                WHERE object_name IN ('STAGE_PLAN', 'STAGE_PROF','DATAPUMP_LOG','ERROR_LOG')
                AND object_type IN ('TABLE')
            """
            
            # SQL to create table STAGE_PLAN dummy table
            create_plan_tab = """
                        CREATE TABLE HP_DIAG.STAGE_PLAN
               (    VERSION NUMBER, 
                SIGNATURE NUMBER, 
                SQL_HANDLE VARCHAR2(30 BYTE), 
                OBJ_NAME VARCHAR2(128 BYTE), 
                OBJ_TYPE VARCHAR2(30 BYTE), 
                PLAN_ID NUMBER, 
                SQL_TEXT CLOB, 
                CREATOR VARCHAR2(128 BYTE), 
                ORIGIN VARCHAR2(30 BYTE), 
                DESCRIPTION VARCHAR2(500 BYTE), 
                DB_VERSION VARCHAR2(64 BYTE), 
                CREATED TIMESTAMP (6), 
                LAST_MODIFIED TIMESTAMP (6), 
                LAST_EXECUTED TIMESTAMP (6), 
                LAST_VERIFIED TIMESTAMP (6), 
                STATUS NUMBER, 
                OPTIMIZER_COST NUMBER, 
                MODULE VARCHAR2(64 BYTE), 
                ACTION VARCHAR2(64 BYTE), 
                EXECUTIONS NUMBER, 
                ELAPSED_TIME NUMBER, 
                CPU_TIME NUMBER, 
                BUFFER_GETS NUMBER, 
                DISK_READS NUMBER, 
                DIRECT_WRITES NUMBER, 
                ROWS_PROCESSED NUMBER, 
                FETCHES NUMBER, 
                END_OF_FETCH_COUNT NUMBER, 
                CATEGORY VARCHAR2(128 BYTE), 
                SQLFLAGS NUMBER, 
                TASK_ID NUMBER, 
                TASK_EXEC_NAME VARCHAR2(128 BYTE), 
                TASK_OBJ_ID NUMBER, 
                TASK_FND_ID NUMBER, 
                TASK_REC_ID NUMBER, 
                INUSE_FEATURES NUMBER, 
                PARSE_CPU_TIME NUMBER, 
                PRIORITY NUMBER, 
                OPTIMIZER_ENV RAW(2000), 
                BIND_DATA RAW(2000), 
                PARSING_SCHEMA_NAME VARCHAR2(128 BYTE), 
                COMP_DATA CLOB, 
                STATEMENT_ID VARCHAR2(30 BYTE), 
                XPL_PLAN_ID NUMBER, 
                TIMESTAMP DATE, 
                REMARKS VARCHAR2(4000 BYTE), 
                OPERATION VARCHAR2(30 BYTE), 
                OPTIONS VARCHAR2(255 BYTE), 
                OBJECT_NODE VARCHAR2(128 BYTE), 
                OBJECT_OWNER VARCHAR2(128 BYTE), 
                OBJECT_NAME VARCHAR2(128 BYTE), 
                OBJECT_ALIAS VARCHAR2(261 BYTE), 
                OBJECT_INSTANCE NUMBER, 
                OBJECT_TYPE VARCHAR2(30 BYTE), 
                OPTIMIZER VARCHAR2(255 BYTE), 
                SEARCH_COLUMNS NUMBER, 
                ID NUMBER, 
                PARENT_ID NUMBER, 
                DEPTH NUMBER, 
                POSITION NUMBER, 
                COST NUMBER, 
                CARDINALITY NUMBER, 
                BYTES NUMBER, 
                OTHER_TAG VARCHAR2(255 BYTE), 
                PARTITION_START VARCHAR2(255 BYTE), 
                PARTITION_STOP VARCHAR2(255 BYTE), 
                PARTITION_ID NUMBER, 
                DISTRIBUTION VARCHAR2(30 BYTE), 
                CPU_COST NUMBER(38,0), 
                IO_COST NUMBER(38,0), 
                TEMP_SPACE NUMBER(38,0), 
                ACCESS_PREDICATES VARCHAR2(4000 BYTE), 
                FILTER_PREDICATES VARCHAR2(4000 BYTE), 
                PROJECTION VARCHAR2(4000 BYTE), 
                TIME NUMBER(38,0), 
                QBLOCK_NAME VARCHAR2(128 BYTE), 
                OTHER_XML CLOB
               ) 
             LOB (SQL_TEXT) STORE AS BASICFILE (
              ENABLE STORAGE IN ROW CHUNK 8192 RETENTION 
              NOCACHE LOGGING 
              STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
              PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
              BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
             LOB (COMP_DATA) STORE AS BASICFILE (
              ENABLE STORAGE IN ROW CHUNK 8192 RETENTION 
              NOCACHE LOGGING 
              STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
              PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
              BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
             LOB (OTHER_XML) STORE AS BASICFILE (
              ENABLE STORAGE IN ROW CHUNK 8192 RETENTION 
              NOCACHE LOGGING 
              STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
              PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
              BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT))
                
            """

            # SQL to create table stage_prof dummy table
            create_prof_tab = """
                            CREATE TABLE HP_DIAG.STAGE_PROF 
               (    VERSION NUMBER, 
                SIGNATURE NUMBER, 
                SQL_HANDLE VARCHAR2(30 BYTE), 
                OBJ_NAME VARCHAR2(128 BYTE), 
                OBJ_TYPE VARCHAR2(30 BYTE), 
                PLAN_ID NUMBER, 
                SQL_TEXT CLOB, 
                CREATOR VARCHAR2(128 BYTE), 
                ORIGIN VARCHAR2(30 BYTE), 
                DESCRIPTION VARCHAR2(500 BYTE), 
                DB_VERSION VARCHAR2(64 BYTE), 
                CREATED TIMESTAMP (6), 
                LAST_MODIFIED TIMESTAMP (6), 
                LAST_EXECUTED TIMESTAMP (6), 
                LAST_VERIFIED TIMESTAMP (6), 
                STATUS NUMBER, 
                OPTIMIZER_COST NUMBER, 
                MODULE VARCHAR2(64 BYTE), 
                ACTION VARCHAR2(64 BYTE), 
                EXECUTIONS NUMBER, 
                ELAPSED_TIME NUMBER, 
                CPU_TIME NUMBER, 
                BUFFER_GETS NUMBER, 
                DISK_READS NUMBER, 
                DIRECT_WRITES NUMBER, 
                ROWS_PROCESSED NUMBER, 
                FETCHES NUMBER, 
                END_OF_FETCH_COUNT NUMBER, 
                CATEGORY VARCHAR2(128 BYTE), 
                SQLFLAGS NUMBER, 
                TASK_ID NUMBER, 
                TASK_EXEC_NAME VARCHAR2(128 BYTE), 
                TASK_OBJ_ID NUMBER, 
                TASK_FND_ID NUMBER, 
                TASK_REC_ID NUMBER, 
                INUSE_FEATURES NUMBER, 
                PARSE_CPU_TIME NUMBER, 
                PRIORITY NUMBER, 
                OPTIMIZER_ENV RAW(2000), 
                BIND_DATA RAW(2000), 
                PARSING_SCHEMA_NAME VARCHAR2(128 BYTE), 
                COMP_DATA CLOB, 
                STATEMENT_ID VARCHAR2(30 BYTE), 
                XPL_PLAN_ID NUMBER, 
                TIMESTAMP DATE, 
                REMARKS VARCHAR2(4000 BYTE), 
                OPERATION VARCHAR2(30 BYTE), 
                OPTIONS VARCHAR2(255 BYTE), 
                OBJECT_NODE VARCHAR2(128 BYTE), 
                OBJECT_OWNER VARCHAR2(128 BYTE), 
                OBJECT_NAME VARCHAR2(128 BYTE), 
                OBJECT_ALIAS VARCHAR2(261 BYTE), 
                OBJECT_INSTANCE NUMBER, 
                OBJECT_TYPE VARCHAR2(30 BYTE), 
                OPTIMIZER VARCHAR2(255 BYTE), 
                SEARCH_COLUMNS NUMBER, 
                ID NUMBER, 
                PARENT_ID NUMBER, 
                DEPTH NUMBER, 
                POSITION NUMBER, 
                COST NUMBER, 
                CARDINALITY NUMBER, 
                BYTES NUMBER, 
                OTHER_TAG VARCHAR2(255 BYTE), 
                PARTITION_START VARCHAR2(255 BYTE), 
                PARTITION_STOP VARCHAR2(255 BYTE), 
                PARTITION_ID NUMBER, 
                DISTRIBUTION VARCHAR2(30 BYTE), 
                CPU_COST NUMBER(38,0), 
                IO_COST NUMBER(38,0), 
                TEMP_SPACE NUMBER(38,0), 
                ACCESS_PREDICATES VARCHAR2(4000 BYTE), 
                FILTER_PREDICATES VARCHAR2(4000 BYTE), 
                PROJECTION VARCHAR2(4000 BYTE), 
                TIME NUMBER(38,0), 
                QBLOCK_NAME VARCHAR2(128 BYTE), 
                OTHER_XML CLOB
               ) 
             LOB (SQL_TEXT) STORE AS BASICFILE (
              ENABLE STORAGE IN ROW CHUNK 8192 RETENTION 
              NOCACHE LOGGING 
              STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
              PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
              BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
             LOB (COMP_DATA) STORE AS BASICFILE (
              ENABLE STORAGE IN ROW CHUNK 8192 RETENTION 
              NOCACHE LOGGING 
              STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
              PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
              BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
             LOB (OTHER_XML) STORE AS BASICFILE (
              ENABLE STORAGE IN ROW CHUNK 8192 RETENTION 
              NOCACHE LOGGING 
              STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
              PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
              BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)) 
            """
            
            # SQL to create external table datapump_log
            create_dpump_log = """
               CREATE TABLE HP_DIAG.DATAPUMP_LOG
                (
                DATABASE_NAME VARCHAR2(50), 
                TIMESTAMP VARCHAR2(50) , 
                DIRECTORY VARCHAR2(250), 
                DUMPFILE VARCHAR2(60), 
                TYPE VARCHAR2(50), 
                TABLE_NAME VARCHAR2(30)
                )
                ORGANIZATION EXTERNAL
                (
                TYPE ORACLE_LOADER
                DEFAULT DIRECTORY KEEP_INFO_REFRESH
                ACCESS PARAMETERS
                (
                    RECORDS DELIMITED BY NEWLINE
                    FIELDS TERMINATED BY ','
                    MISSING FIELD VALUES ARE NULL
                )
                LOCATION ('datapump_log.csv')
                )
                REJECT LIMIT UNLIMITED
            """

            create_error_log = """
                CREATE table HP_DIAG.error_log
                (
                DATABASE_NAME VARCHAR2(50)
                ,error_code NUMBER(*,0)
                ,  error_message VARCHAR2(4000 byte)
                ,  backtrace VARCHAR2(4000 byte)
                ,  callstack VARCHAR2(4000 byte)
                ,  created_on date
                ,  created_by varchar2(100)
                )
                ORGANIZATION EXTERNAL
                (
                TYPE ORACLE_LOADER
                DEFAULT DIRECTORY KEEP_INFO_REFRESH
                ACCESS PARAMETERS
                (
                    RECORDS DELIMITED BY '*'
                    FIELDS TERMINATED BY '|'
                    MISSING FIELD VALUES ARE NULL
                )
                LOCATION ('error_log.csv')
                )
                REJECT LIMIT UNLIMITED
            """

            try:
                cursor = self.connection.cursor()
                cursor.arraysize = 1000
                cursor.execute(sql_check)
                existing_objects = {row[0] for row in cursor.fetchall()}  # i am using a set for efficient look-up
                logger.info(f"{db.get_name()} started...")
                # Create tables if they do not exist
                if 'STAGE_PLAN' not in existing_objects:
                    logger.info(f"Creating table HP_DIAG.STAGE_PLAN...")
                    cursor.execute(create_plan_tab)
                    logger.info(f"Table HP_DIAG.STAGE_PLAN created successfully.")
                elif 'STAGE_PLAN' in existing_objects:
                    logger.info(f"Table HP_DIAG.STAGE_PLAN already exists.")
                if 'STAGE_PROF' not in existing_objects:
                    logger.info(f"Creating table HP_DIAG.STAGE_PROF...")
                    cursor.execute(create_prof_tab)
                    logger.info(f"Table HP_DIAG.STAGE_PROF created successfully.")
                elif 'STAGE_PROF' in existing_objects:
                    logger.info(f"Table HP_DIAG.STAGE_PROF already exists.")
                if 'DATAPUMP_LOG' not in existing_objects:
                    logger.info(f"Creating table HP_DIAG.DATAPUMP_LOG...")
                    cursor.execute(create_dpump_log)
                    logger.info(f"Table HP_DIAG.DATAPUMP_LOG created successfully.")
                elif 'DATAPUMP_LOG' in existing_objects:
                    logger.info(f"Table HP_DIAG.DATAPUMP_LOG already exists.")
                    logger.info(f"Drop and recreate HP_DIAG.DATAPUMP_LOG anyway!")
                    cursor.execute('DROP TABLE HP_DIAG.DATAPUMP_LOG')
                    cursor.execute(create_dpump_log)
                    logger.info(f"Table HP_DIAG.DATAPUMP_LOG created successfully.")
                if 'ERROR_LOG' not in existing_objects:
                    logger.info(f"Creating table HP_DIAG.ERROR_LOG...")
                    cursor.execute(create_error_log)
                    logger.info(f"Table HP_DIAG.ERROR_LOG created successfully.")
                elif 'ERROR_LOG' in existing_objects:
                    logger.info(f"Table HP_DIAG.ERROR_LOG already exists.")
                    logger.info(f"Drop and recreate HP_DIAG.ERROR_LOG anyway!")
                    cursor.execute('DROP TABLE HP_DIAG.ERROR_LOG')
                    cursor.execute(create_error_log)
                    logger.info(f"Table HP_DIAG.ERROR_LOG created successfully.")

                
                logger.info(f"{db.get_name()} ended...")
                    
                    
            except Exception as e:
                logger.exception(f"Error in creating HP_DIAG objects: {str(e)}")
                logger.info(f"{db.get_name()} ended...")
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
                logger.info(f"NOTE: MINIMUM FREE PERCENT  = {self.min_percent}")
                logger.info(f"DEBUG: Number of databases being checked:, {len(self.databases)}")
                for db in self.databases:  # Loop through the databases       
                    self.process_database(db)       
            else:
                logger.info(f"Failed to extract the command line aruguments into variables")

if __name__ == "__main__":
    oracle_monitor = OracleTablespaceMonitorFreeSpaceNonFocus()
    #print("Entering run() method for main class.")
    oracle_monitor.run()
