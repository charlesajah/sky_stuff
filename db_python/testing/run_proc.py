
import oracledb
import sys
import os
import platform
import pdb
import re
import functools
import math
print (sys.path)
from datapump import Datapump
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

logger.info(f"Starting export and/or import of SQL Plans and SQL Profiles")

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

class RunProcedure(Datapump):
    QUERY_TIMEOUT = 30
    LOGIN_TIMEOUT = 30

    def __init__(self):
        super().__init__()
        #self.min_percent = 3.0
        self.tns=""
        self.dns=""
        self.host_val=""
        self.port_val=0
        self.service_val=""
        self.user_val=""
        self.pword_val=""
        self.db_name=""
        self.connection= None
            

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
        self.run_procedure(db, self.datapump_type)
        cursor.close()
        self.connection.close()
        pool.close()
        db_available = 1
        logger.info(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
        
 
    
    def run_procedure(self, db, datapump_type):
        try:
            cursor = self.connection.cursor()
            cursor.arraysize = 1000
            logger.info(f"{db.get_name()} started...")
            #we enable DBMS_OUTPUT
            cursor.callproc('DBMS_OUTPUT.ENABLE')
            if datapump_type == 'export':
                cursor.callproc('preserve_prof_plan_baselines.export_plan_baselines')
                # Fetch and print DBMS_OUTPUT messages
                self.print_dbms_output(cursor)
                cursor.callproc('preserve_prof_plan_baselines.export_profiles')
                # Fetch and print DBMS_OUTPUT messages
                self.print_dbms_output(cursor)
                logger.info(f"{db.get_name()} ended...")
            elif datapump_type == 'import':
                cursor.callproc('preserve_prof_plan_baselines.import_plan_baselines')
                # Fetch and print DBMS_OUTPUT messages
                self.print_dbms_output(cursor)
                cursor.callproc('preserve_prof_plan_baselines.import_profiles')
                # Fetch and print DBMS_OUTPUT messages
                self.print_dbms_output(cursor)
                logger.info(f"{db.get_name()} ended...")
        except Exception as e:
            logger.exception(f"Error in executing  HP_DIAG proc: {str(e)}")
           
        finally:
            cursor.close()  # Ensure the cursor is closed after operation 

    def print_dbms_output(self, cursor):
        # tune this size for your application
        chunk_size = 100
        # create variables to hold the output
        lines_var = cursor.arrayvar(str, chunk_size)
        num_lines_var = cursor.var(int)
        num_lines_var.setvalue(0, chunk_size)

        #fetch the text that was added by the procedure
        while True:
            cursor.callproc("dbms_output.get_lines", (lines_var, num_lines_var))
            num_lines = num_lines_var.getvalue()
            lines = lines_var.getvalue()[:num_lines]
            for line in lines:
                print(line or "")
            if num_lines < chunk_size:
                break
    

    def check_args(self, args):
        if args is not None:
            try:              
                # Ensure that the value string supplied is either 'export' or 'import', and is not empty
                self.datapump_type = args[2:]
                size = len(self.datapump_type)
                self.datapump_type = self.datapump_type[1]
                if hasattr(self, 'datapump_type') and self.datapump_type:
                    logger.info(f"-a value supplied is {self.datapump_type}")
                    logger.debug(f"datapump_type before stripping: '{self.datapump_type}'")
                    if not self.datapump_type.strip():  # Check if the string is empty after stripping whitespace
                        raise ValueError("Argument -a cannot be an empty string.")
                    return True  # Return True when parsing is successful                               
                    
            except Exception as e:
                logger.exception(f"Floating error is :, {e}")
                return False
        

    def run(self):
        logger.info(f"NOTE: QUERY TIMEOUT      = {self.QUERY_TIMEOUT} seconds")
        logger.info(f"NOTE: CONNECTION TIMEOUT = {self.LOGIN_TIMEOUT} seconds")
        args = sys.argv[1:]  # Get command-line arguments here
        #print("The arguments:", args)
        logger.info(f"The arguments are: {args}")
        if self.check_args(args):
            if self.populate():
                logger.info(f"NOTE: MINIMUM FREE PERCENT  = {self.min_percent}")
                logger.info(f"DEBUG: Number of databases being checked:, {len(self.databases)}")
                for db in self.databases:  # Loop through the databases       
                    self.process_database(db)       
            else:
                logger.info(f"Failed to extract the command line aruguments into variables")

if __name__ == "__main__":
    data_pump = RunProcedure()
    #print("Entering run() method for main class.")
    data_pump.run()
