
print("Starting OracleTablespaceMonitorFreeSpaceNonFocus.py")  # Add this line at the beginning
import oracledb
import sys
import os
import platform
import pdb
import re
import functools
print (sys.path)
from oraclemonitor import OracleMonitor

if sys.maxsize > 2147483647:

  print("Running in 64-bit mode.")

else:

  print("Not running in 64-bit mode.")

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
        self.user_val=""
        self.pword_val=""
        self.db_name=""
        self.connection= None
        #self.cursor= None
        #self.rec_cursor = None
        
        

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
    def check_recovery_area(self, db, rec_area_cursor):
        for row in self.rec_cursor:
            f_type= row[0]
            percent_space_used= row[1]
            percent_space_reclaimable = row[2]
            
    def db_conn(self, db):
        try:
            #self.connection = oracledb.connect(user=self.user_val, password=self.pword_val, host=self.host_val, service_name=self.service_val, port=self.port_val)
            print("user=",self.user_val," password=",self.pword_val," host=",self.host_val," service_name=",self.service_val, "port=",self.port_val)
            self.connection = oracledb.connect(user=self.user_val, password=self.pword_val, dsn=db.get_name())
        except oracledb.DatabaseError as error:
            print("Database connection error encountered:", error)
            db_available = 0 
            #print(f"CONN ERROR: {str(error)}")
            print("Conn Details are:" , self.host_val, self.port_val,self.service_val)
            print(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
        
        
        

    def process_database(self, db):
        print(f"NOTE: Processing {db.get_name()}, tns={db.get_tns()} ...")
        #print("Service name is :" , service_val)  
        self.db_name=db.get_name()
        dns=db.get_name()
        self.user_val=db.get_username()
        self.pword_val=db.get_password()
        #print ("TNS String is:", dns)
        #self.split_conn_string(dns,db)
        
        #self.db_conn
        
        try:
            #self.connection = oracledb.connect(user=self.user_val, password=self.pword_val, host=self.host_val, service_name=self.service_val, port=self.port_val)
            #The following Standalone connection works but will fail if i tried to add the timeout parameter
            #connection = oracledb.connect(user=self.user_val, password=self.pword_val, dsn=db.get_name())
            
            #In order to be able to implement connection TIME_OUT i had to to use connection pooling as this is not avaiable with Standalone connections
            pool = oracledb.create_pool(user=self.user_val, password=self.pword_val, dsn=db.get_name(),min=1, max=3, increment=1, timeout=self.LOGIN_TIMEOUT)
            self.connection = pool.acquire()
        except oracledb.DatabaseError as error:
            print("Database connection error encountered:", error)
            db_available = 0 
            #print(f"CONN ERROR: {str(error)}")
            #print("Conn Details are:" , self.host_val, self.port_val,self.service_val)
            print(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
            return
      
       
        #connection = oracledb.connect(user=u_name, password=pword, host=host_name, service_name=s_name) 
                   
        cursor = self.connection.cursor()
        cursor.arraysize = 1000
        db_available = 1        

        try:
            #print("Query contains something?")
            cursor.execute(self.query)
            print("Collecting Tablespace data ...")
            #print(f"Checking value for db.get_percent() :{db.get_percent()}")
            
            for row in cursor:
                tablespace = row[0]
                tspace_size= row[2]
                pct_free = row[3]
                #print("Verifiying we have entered the Handle_Tablespace")
                
                if not db.is_tablespace_excluded(tablespace):
                    cus_pct_free=db.get_percent()
                    if cus_pct_free > 0.0:
                        check_value=cus_pct_free
                        
                    else:
                        check_value = self.min_percent if self.min_percent > 0.0 else db.get_percent()
                    
                                      

                    if float(pct_free) < check_value:
                        #print("The Pct_Free value is :",float(pct_free))
                        #print("The Check value is :", check_value)
                        print(f"_datapoint:DatabaseTablespace:{db.get_name()}_{tablespace}:freepct:{pct_free}")
                        #self.add_datafiles(self, db,tablespace, pct_free, check_value, tspace_size)
                        self.add_datafiles(db,tablespace, pct_free, check_value, tspace_size)
                        
        except Exception as e:
            print(f"Tablespace Check Error: {str(e)}")
            return
            
        print("Collecting Recovery Area data ...")
        try:
            cursor.execute("""select * from V$RECOVERY_AREA_USAGE""")  
        except Exception as error:
            print(f"V$RECOVERY_AREA_USAGE: {str(error)}")           
            print(db.get_name(),":V$RECOVERY_AREA_USAGE is not available on 10g versions")
            #adding a return clause here to terminally catch any error raised from trying to access V$RECOVERY_AREA_USAGE
            #as this annoyingly kept going out of the scope of this local/inner try block to the outer try block as well
            print(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
            return
        for row in cursor:
            f_type = row[0]
            percent_space_used = row[1]
            percent_space_reclaimable = row[2]                
            if percent_space_used > 50.00:
                print(f"_datapoint:FIleType:{f_type}:{db.get_name()}:%used:{percent_space_used}")
                if percent_space_reclaimable > 0:
                    print(f"_datapoint:FIleType:{f_type}:{db.get_name()}:%percent_space_reclaimable:{percent_space_reclaimable}")
                
            
        
                

        cursor.close()
            
            #rec_cursor = connection.cursor()
            #rec_cursor.arraysize = 1000
            #rec_cursor.execute('select * from V$RECOVERY_AREA_USAGE')
            
            #for row in rec_cursor:
                #self.f_type= row[0]
                #self.percent_space_used= row[1]
                #self.percent_space_reclaimable = row[2]
                #if self.percent_space_used > 50.00:
                    #print(f"FIle Type is {self.file_type} and  %Used is:{self.percent_space_used}")
                    
            #rec_cursor.close()

        self.connection.close()
        pool.close()
        db_available = 1
        print(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
        
    def split_conn_string(self, dns, db):
        print("Testing if split_conn_string was entered at all", dns)
    
        try:
            # Define a case-insensitive regular expression pattern to match SERVICE_NAME
            service_name_pattern = r"SERVICE_NAME=([a-zA-Z0-9_-]+)"
    
            # Use re.search with re.IGNORECASE to find the first case-insensitive SERVICE_NAME match
            service_name_match = re.search(service_name_pattern, dns, re.IGNORECASE)
    
            if service_name_match:
                # Extract the SERVICE_NAME value from the match
                self.service_val = service_name_match.group(1)
                
                print("SERVICE_NAME Value:", self.service_val)
                self.split_host_port(dns,db)
                
            else:
                self.service_val=db.get_name()
                print("Conn string has no given service_name so default Service_name is:", self.service_val)         
                self.split_host_port(dns,db)
                self.service_val=db.get_name()
                           
        except Exception as e:
            print(f"Catching tns error: {str(e)}")
    
    def convert_to_megabytes(self, size_str):
        units = {'B': 1 / (1024 ** 2), 'K': 1 / 1024, 'M': 1, 'G': 1024}
        size_str = size_str.upper()
        unit = size_str[-1]
        if unit in units:
            return float(size_str[:-1]) * units[unit]
        else:
            return float(size_str)    
            

    def add_datafiles(self, db,tablespace, pct_free, check_value, tspace_size):
        #print("Verifiying we have entered self.add_datafiles")
        #tablespace=tablespace
        try:
            cursor = self.connection.cursor()
            cursor.arraysize = 1000
        
        except Exception as e:
            print(f"Catching function add_datafiles cursor error: {str(e)}")

        try:
            #check if ASM is configured for the database
            cursor.execute("""select count(*) from V$ASM_DISKGROUP""")  
        except Exception as e:
            print(f"ASM verification query failed : {str(e)}")
            return
        
        for row in cursor:
                cnt = row[0]
                if cnt==0:
                    sql = (
                            'SELECT '
                            'ext.mounted_on, '
                            'ext.fsize, '
                            'ext.used, '
                            'ext.Avail, '
                            'ext.use_pcent, '
                            'df.tablespace_name '
                            'FROM  dev.filesystem ext '
                            'JOIN   dba_data_files df '
                            'ON '
                            'REGEXP_LIKE (ext.mounted_on, SUBSTR(df.file_name, 1, INSTR(df.file_name, \'/\', -1, 2) - 1)) '
                            'AND df.tablespace_name= :tspace '
                            'AND df.autoextensible= :xtensible '
                            'group by  df.tablespace_name,ext.mounted_on, '
                            'ext.fsize, '
                            'ext.used, '
                            'ext.Avail, '
                            'ext.use_pcent '
                            'order by tablespace_name '
                            
                        )

                    try:
                        #query to find if space exists on OS mount points
                        #it is good coding practice to use param binding in order to avoid SQL injection
                        #we bind tablespace to tspace and NO to xtensible here
                        cursor.execute(sql, {'tspace':tablespace, 'xtensible':'NO'})  
                    except Exception as e:
                        print(f"Mountpoint verification query failed : {str(e)}")
                        return
                    
                    # Calculate the additional space needed in megabytes to bring it level to required_min_free_pct 
                    # and add 10% to the required_min_free_pct
                    required_min_free_pct = check_value
                    additional_space_needed_mb = max(0, (tspace_size * (required_min_free_pct / 100) - tspace_size * (pct_free / 100))) + 0.1 * (tspace_size * (required_min_free_pct / 100))

                    tablespace_data = {}  # dictionary to store all tablespace  mount point data
                    selected_mountpoints_data = {}  # New dictionary for storing mount points big enough to contain additional_space_needed_mb
                    for row in cursor:
                        mpoint=row[0]
                        mpoint_size=row[1]
                        mpoint_used=row[2]
                        mpoint_avail=self.convert_to_megabytes(row[3])  # Convert to megabytes
                        mpoint_use_pcent=row[4]
                        tspace_name=row[5]

                        #print(f" Mount Point  {mpoint}  has {mpoint_avail} available for tablespace {tspace_name}")

                        # Check if the mount point has enough available space
                        if mpoint_avail > additional_space_needed_mb:
                            entry1 = {
                                'tablespace': tspace_name,
                                'mountpoint': mpoint,
                                'available_space': mpoint_avail,
                            }
                            # Store the entry in the selected_mountpoints_data dictionary
                            selected_mountpoints_data[tspace_name] = entry1 # Use tablespace name as the key
                        
                        # Check if selected_mountpoints_data has entries
                            if len(selected_mountpoints_data) > 0:
                                for key, entry1 in selected_mountpoints_data.items():
                                    mpoint_avail_mb = entry1['available_space']
                                    print(f"Tablespace {tablespace} requires {additional_space_needed_mb} MB of more space, "
                                        f"and there is more than enough available space on mount point {entry1['mountpoint']} ")
                            
                        else:
                            print(f"Required Tablespace space for {tablespace} cannot go wholly into the mount point {entry1['mpoint']}")

                        
                        # Create a dictionary for each entry
                        entry2 = {
                            'tablespace': tspace_name,
                            'mountpoint': mpoint,
                            'available_space': mpoint_avail,
                        }  
                        
                        tablespace_data[tspace_name] = entry2  # Use tablespace name as the key
                        #print (tablespace_data)
                        for key in tablespace_data.keys():
                            #print(key, ":", tablespace_data[key])
                            #i just want to do nothing here
                            tablespace=tablespace

                        

                    
                        
        #this cursor.close looks out of scope to me
        #looks like it is not being used
        #i'd rather close connections from the calling method process_database
        #self.connection is closed from process_database method anyway
        #something to think about
        cursor.close()
        




                    



    def split_host_port(self,dns,db):
        # Define a case-insensitive regular expression pattern to match HOST
        host_pattern = r"HOST=([a-zA-Z0-9.-]+)"
        
        # Use re.findall to find all case-insensitive HOST matches for HOST in the DNS string
        host_matches = re.findall(host_pattern, dns, re.IGNORECASE)
        # Use re.search with re.IGNORECASE to find the first case-insensitive HOST match
        #host_match = re.search(host_pattern, dns, re.IGNORECASE)

        if len(host_matches) > 1:
            print("Hosts found in string", host_matches)
            # Extract each HOST value from the string
            for h in range(len(host_matches)):
                self.host_val = host_matches[h]
                print("Testing connection for HOST:", self.host_val)
                
                # Define a case-insensitive regular expression pattern to match PORT
                port_pattern = r"PORT=(\d+)"
                # Use re.search with re.IGNORECASE to find the first case-insensitive PORT match
                port_match = re.search(port_pattern, dns, re.IGNORECASE)

                if port_match:
                    # Extract the PORT value from the match
                    self.port_val = port_match.group(1)
                    print("PORT Value:", self.port_val)
                    self.db_conn(db)                    
                else:
                    print("PORT not found in the connection string.")
                    
        else:
            print("No multiple hosts found in conn string")
            self.host_val=host_matches[0]
            # Define a case-insensitive regular expression pattern to match PORT
            port_pattern = r"PORT=(\d+)"
            # Use re.search with re.IGNORECASE to find the first case-insensitive PORT match
            port_match = re.search(port_pattern, dns, re.IGNORECASE)

            if port_match:
                # Extract the PORT value from the match
                self.port_val = port_match.group(1)
                print("PORT Value:", self.port_val)
                print("Conn details is : ", self.connection)
                self.db_conn(db)
                
            else:
                print("PORT not found in the connection string.")
        
        
     
        
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
        #print("The arguments:", args)
        if self.check_args(args):
            if self.populate():
                print(f"NOTE: MINIMUM FREE PERCENT  = {self.min_percent}")
                print("DEBUG: Number of databases being checked:", len(self.databases))
                #print("The contents of the query file is :", self.query)
                #for x in range(3):
                for db in self.databases:  # Loop through the databases
                    #print("DBName is:", db.get_name())
                    
                    #if isinstance(db, OracleDatabase):
                    #print("db is an instance of OracleDatabase")
                        
                    #print("Service Name is: ", self.databases[0])
                    #else:
                    #print("Just confirming Object Types", type(self.handle_tablespaces))
                    #print(callable(self.handle_tablespaces))         
                    self.process_database(db)
                    
                    
            else:
                print("Failed to extract the command line aruguments into variables")

if __name__ == "__main__":
    oracle_monitor = OracleTablespaceMonitorFreeSpaceNonFocus()
    #print("Entering run() method for main class.")
    oracle_monitor.run()
