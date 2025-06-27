
print("Starting OracleTablespaceMonitorFreeSpaceNonFocus.py")  # Add this line at the beginning
import oracledb
import sys
import os
import platform
import pdb
import re
import functools
from oraclemonitor import OracleMonitor

d = None  # default suitable for Linux
if platform.system() == "Windows":
    d = r"C:\oracle\product\instantclient_19_20"

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

    def handle_tablespaces(self, db, cursor):
        
        for row in cursor:
            tablespace = row[0]
            pct_free = row[3]
            if not db.is_tablespace_excluded(tablespace):
                check_value = self.min_percent if self.min_percent > 0.0 else db.get_percent()
                if float(self.pct_free) < check_value:
                    #print("The Pct_Free value is :",float(pct_free))
                    print(f"_datapoint:DatabaseTablespace:{db.get_name()}_{tablespace}:freepct:{pct_free}")
                    return tablespace, self.pct_free

    def process_database(self, db):
        print(f"NOTE: Processing {db.get_name()}, tns={db.get_tns()} ...")
        service_val=db.get_name()
        print("Service name is :" , service_val)       
        dns=db.get_tns()
        print ("TNS String is:", dns)
        self.split_conn_string(dns)
        connection = oracledb.connect(user=db.get_username(), password=db.get_password(), host=self.host_val, service_name=db.get_name(), port=self.port_val)            
       
        cursor = connection.cursor()
        cursor.arraysize = 1000
        db_available = 1        

        try:
            print("Query contains something?")
            cursor.execute(self.query)
            print("Still Debugging :")
            for row in cursor:
                tablespace = row[0]
                pct_free = row[3]
                print("Verifiying we have entered the Handle_Tablespace")
                
                if not db.is_tablespace_excluded(tablespace):
                    check_value = self.min_percent if self.min_percent > 0.0 else db.get_percent()
                    if float(pct_free) < check_value:
                        print("The Pct_Free value is :",float(pct_free))
                        print("The Check value is :", check_value)
                        print(f"_datapoint:DatabaseTablespace:{db.get_name()}_{tablespace}:freepct:{pct_free}")
            
            
        except Exception as e:
            print(f"QUERY ERROR: {str(e)}")
                

            cursor.close()

        connection.close()
        db_available = 1
        print(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
        
    def split_conn_string(self, dns):
        print("Testing if split_conn_string was enetred at all", dns)
    
        try:
            # Define a case-insensitive regular expression pattern to match SERVICE_NAME
            service_name_pattern = r"SERVICE_NAME=([a-zA-Z0-9_-]+)"
    
            # Use re.search with re.IGNORECASE to find the first case-insensitive SERVICE_NAME match
            service_name_match = re.search(service_name_pattern, dns, re.IGNORECASE)
    
            if service_name_match:
                # Extract the SERVICE_NAME value from the match
                #self.service_val = service_name_match.group(1)
                self.split_host_port(dns)
                print("SERVICE_NAME Value:", self.service_val)
            else:
                print("SERVICE_NAME not found in the sample data bur Service_name is:", self.service_val)
                #self.service_val = service_val
                self.split_host_port(dns)
                
    
            return True
        except Exception as e:
            print(f"Catching tns error: {str(e)}")
    
            return False

     
    def split_host_port(self,dns):
        # Define a case-insensitive regular expression pattern to match HOST
        host_pattern = r"HOST=([a-zA-Z0-9.-]+)"

        # Use re.search with re.IGNORECASE to find the first case-insensitive HOST match
        host_match = re.search(host_pattern, dns, re.IGNORECASE)

        if host_match:
            # Extract the HOST value from the match
            self.host_val = host_match.group(1)
            print("First HOST Value:", self.host_val)
        else:
            print("HOST not found in the sample data.")
            
        # Define a case-insensitive regular expression pattern to match PORT
        port_pattern = r"PORT=(\d+)"
        # Use re.search with re.IGNORECASE to find the first case-insensitive PORT match
        port_match = re.search(port_pattern, dns, re.IGNORECASE)

        if port_match:
            # Extract the PORT value from the match
            self.port_val = port_match.group(1)
            print("PORT Value:", self.port_val)
        else:
            print("PORT not found in the sample data.")
        
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
                    print("Floating error is :", e)
                    return False
        return True  # Return True when parsing is successful

    def run(self):
        print(f"NOTE: QUERY TIMEOUT      = {self.QUERY_TIMEOUT} seconds")
        print(f"NOTE: CONNECTION TIMEOUT = {self.LOGIN_TIMEOUT} seconds")
        args = sys.argv[1:]  # Get command-line arguments here
        print("The arguments:", args)
        if self.check_args(args):
            if self.populate():
                print(f"NOTE: MINIMUM FREE PERCENT  = {self.min_percent}")
                print("DEBUG: Number of databases being checked:", len(self.databases))
                #print("The contents of the query file is :", self.query)
                #for x in range(3):
                for db in self.databases:  # Loop through the database 
                    print("DBName is:", db.get_name())
                    try:
                                #if isinstance(db, OracleDatabase):
                                    #print("db is an instance of OracleDatabase")
                        
                                    #print("Service Name is: ", self.databases[0])
                                #else:
                        print("Just confirming Object Types", type(self.handle_tablespaces))
                        print(callable(self.handle_tablespaces))         
                        self.process_database(db)
                    except oracledb.DatabaseError as error:
                        print("Database connection error encountered:", error)
                        db_available = 0 
                        print(f"CONN ERROR: {str(error)}")
                        print("Conn Details are:" , self.host_val, self.port_val)
                        print(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
                    
            else:
                print("Failed to extract the command line aruguments into variables")

if __name__ == "__main__":
    oracle_monitor = OracleTablespaceMonitorFreeSpaceNonFocus()
    print("Entering run() method for main class.")
    oracle_monitor.run()
