
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

logger.info(f"Starting OracleTablespaceMonitorFreeSpaceNonFocus.py")

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
        
        

    def handle_tablespaces(self, db, cursor):
        
        for row in cursor:
            tablespace = row[0]
            pct_free = row[3]
            if not db.is_tablespace_excluded(tablespace):
                check_value = self.min_percent if self.min_percent > 0.0 else db.get_percent()
                if float(self.pct_free) < check_value:
                    #print("The Pct_Free value is :",float(pct_free))
                    logger.info(f"_datapoint:DatabaseTablespace:{db.get_name()}_{tablespace}:freepct:{pct_free}")
                    return tablespace, self.pct_free
                
    def check_recovery_area(self, db, rec_area_cursor):
        for row in self.rec_cursor:
            f_type= row[0]
            percent_space_used= row[1]
            percent_space_reclaimable = row[2]
            
    def db_conn(self, db):
        try:
            #self.connection = oracledb.connect(user=self.user_val, password=self.pword_val, host=self.host_val, service_name=self.service_val, port=self.port_val)
            logger.info("user=",self.user_val," password=",self.pword_val," host=",self.host_val," service_name=",self.service_val, "port=",self.port_val)
            self.connection = oracledb.connect(user=self.user_val, password=self.pword_val, dsn=db.get_name())
        except oracledb.DatabaseError as error:
            logger.exception(f"Database connection error encountered:, {error}")
            db_available = 0 
            #print(f"CONN ERROR: {str(error)}")
            logger.info(f"Conn Details are: , {self.host_val}, {self.port_val},{self.service_val}")
            logger.info(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
               

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

        try:
            #print("Query contains something?")
            cursor.execute(self.query)
            logger.info(f"Collecting Tablespace data ...")
            
            for row in cursor:
                tablespace = row[0]
                free =  row[1]
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
                        logger.info(f"_datapoint:DatabaseTablespace:{db.get_name()}_{tablespace}:freepct:{pct_free}")
                        #self.add_datafiles(self, db,tablespace, pct_free, check_value, tspace_size)
                        self.add_datafiles(db,tablespace, free, pct_free, check_value, tspace_size)
                        
        except Exception as e:
            logger.exception(f"Tablespace Check Error: {str(e)}")
            return
            
        logger.info(f"Collecting Recovery Area data ...")
        try:
            cursor.execute("""select * from V$RECOVERY_AREA_USAGE""")  
        except Exception as error:
            logger.exception(f"V$RECOVERY_AREA_USAGE: {str(error)}")           
            logger.info(f"{db.get_name()},:V$RECOVERY_AREA_USAGE is not available on 10g versions")
            #adding a return clause here to terminally catch any error raised from trying to access V$RECOVERY_AREA_USAGE
            #as this annoyingly kept going out of the scope of this local/inner try block to the outer try block as well
            logger.info(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
            return
        for row in cursor:
            f_type = row[0]
            percent_space_used = row[1]
            percent_space_reclaimable = row[2]                
            if percent_space_used > 50.00:
                logger.info(f"_datapoint:FIleType:{f_type}:{db.get_name()}:%used:{percent_space_used}")
                if percent_space_reclaimable > 0:
                    logger.info(f"_datapoint:FIleType:{f_type}:{db.get_name()}:%percent_space_reclaimable:{percent_space_reclaimable}")               

        cursor.close()
        self.connection.close()
        pool.close()
        db_available = 1
        logger.info(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
        
    def split_conn_string(self, dns, db):
        logger.info(f"Testing if split_conn_string was entered at all", dns)
    
        try:
            # Define a case-insensitive regular expression pattern to match SERVICE_NAME
            service_name_pattern = r"SERVICE_NAME=([a-zA-Z0-9_-]+)"
    
            # Use re.search with re.IGNORECASE to find the first case-insensitive SERVICE_NAME match
            service_name_match = re.search(service_name_pattern, dns, re.IGNORECASE)
    
            if service_name_match:
                # Extract the SERVICE_NAME value from the match
                self.service_val = service_name_match.group(1)
                
                logger.info(f"SERVICE_NAME Value:, {self.service_val}")
                self.split_host_port(dns,db)
                
            else:
                self.service_val=db.get_name()
                logger.info(f"Conn string has no given service_name so default Service_name is:, {self.service_val}")         
                self.split_host_port(dns,db)
                self.service_val=db.get_name()
                           
        except Exception as e:
            logger.exception(f"Catching tns error: {str(e)}")
    
    def convert_to_megabytes(self, size_str):
        units = {'B': 1 / (1024 ** 2), 'K': 1 / 1024, 'M': 1, 'G': 1024, 'T': 1024 ** 2}
        size_str = size_str.upper()
        unit = size_str[-1]
        if unit in units:
            return float(size_str[:-1]) * units[unit]
        else:
            return float(size_str)

            
    def process_datafile_mpoints(self, tspace_name):
        # When all the required tablespace space to be allocated
        # cannot be wholly allocated on any of the mountpoints currently being used by the current tablespace.
        #or it cannot be allocated wholly into any eligible mount points
        # we now try to split it up and spread it across multiple mountpoints using this function when it can't fit into a single eligible mount point.
        logger.info(f"Total additional space needed: {self.additional_space_needed_mb} MB")       

        try:
            cursor = self.connection.cursor()
            sql=(
                        'SELECT '
                            'distinct mounted_on, '
                            'fsize, '
                            'used, '
                            'Avail, '
                            'use_pcent '  
                            'from( '
                        'SELECT '
                            'mounted_on, '
                            'fsize, '
                            'used, '
                            'Avail, '
                            'use_pcent, '
                            'tablespace_name '
                        'FROM ('
                            'SELECT '
                            'ext.mounted_on, '
                            'ext.fsize, '
                            'ext.used, '
                            'ext.Avail, '
                            'ext.use_pcent, '
                            'df.tablespace_name, '
                            'ROW_NUMBER() OVER ('
                                'PARTITION BY df.file_name '
                                'ORDER BY LENGTH(ext.mounted_on) DESC'
                            ') as rn '
                            'FROM dev.filesystem ext '
                            'JOIN dba_data_files df ON df.file_name LIKE \'%\' || ext.mounted_on || \'%\' '
                            'WHERE ext.mounted_on != \'/\''
                        ') subquery '
                        'WHERE rn = 1 '
                        'GROUP BY tablespace_name, mounted_on, fsize, used, Avail, use_pcent '
                        'ORDER BY tablespace_name)'
                    )
            cursor.execute(sql)

            for row in cursor:
                mp_size = self.convert_to_megabytes(row[1])
                mp_used = self.convert_to_megabytes(row[2])
                mp_avail = self.convert_to_megabytes(row[3])
                os_mp = row[0]

                entry = {
                    'mountpoint': os_mp,
                    'available_space': mp_avail,
                    'mpoint_size': mp_size,
                    'mpoint_used': mp_used,
                }

                self.tablespace_data[os_mp] = entry

            #cursor.close()
        except Exception as e:
            logger.info(f"Error querying filesystem: {e}")
            return

        for mpoint_key, data in self.tablespace_data.items():
            max_allowable_space = data['mpoint_size'] * 0.95 - data['mpoint_used']
            if max_allowable_space > 0 and self.remaining_space_needed > 0:

                # in this while loop we ensure file size is no more than self.MAX_FILE_SIZE_MB
                while self.remaining_space_needed > 0:
                    space_to_allocate = min(self.remaining_space_needed, max_allowable_space)
                    new_datafile=self.generate_new_datafile_name(tspace_name, mpoint_key, self.generated_filenames) #Generate new datafile name before allocating space
                    if new_datafile == None:
                        break
                    size_mb=min(math.ceil(space_to_allocate), self.MAX_FILE_SIZE_MB)
                    if self.MAX_FILE_SIZE_MB < max_allowable_space :
                        #then we override size_mb and make size_mb same size as self.MAX_FILE_SIZE_MB
                        size_mb= self.MAX_FILE_SIZE_MB
                        space_to_allocate=size_mb
                    else:
                        #then we override size_mb and make size_mb same size as max_allowable_space
                        size_mb=math.ceil(max_allowable_space)
                        space_to_allocate=size_mb
                        
                    if self.create_data_file(tspace_name, new_datafile, size_mb):
                        # Then we adjust the required tablespace size balance
                        self.remaining_space_needed -= size_mb

                        # updates the dictionary (self.space_allocations) that keeps track of how much space has been allocated on each mount point.
                        self.space_allocations[mpoint_key] = self.space_allocations.get(mpoint_key, 0) + space_to_allocate
                        self.tablespace_data[mpoint_key]['available_space'] -= space_to_allocate
        
        if self.remaining_space_needed > 0:
            logger.info(f"Unable to allocate the required {self.additional_space_needed_mb} MB; {self.remaining_space_needed} MB still needed.")
        else:
            for mpoint, allocated_space in self.space_allocations.items():
                logger.info(f"Successfully allocated {allocated_space} MB across mount points.")
                logger.info(f"{allocated_space} MB allocated to mount point {mpoint}")
        return self.remaining_space_needed
            
    def add_datafiles(self, db, tablespace, free, pct_free, check_value, tspace_size):
        try:
            cursor = self.connection.cursor()
            cursor.arraysize = 1000
        except Exception as e:
            logger.exception(f"Catching function add_datafiles cursor error: {str(e)}")
        
        try:
            # check if ASM is configured for the database
            cursor.execute("""select count(*) from V$ASM_DISKGROUP""")
        except Exception as e:
            logger.exception(f"ASM verification query failed : {str(e)}")
            return

        entry1 = None  # Declare entry1

        for row in cursor:
            cnt = row[0]
            if cnt == 0:
                sql = (
                        'SELECT '
                        'mounted_on, '
                        'fsize, '
                        'used, '
                        'Avail, '
                        'use_pcent, '
                        'tablespace_name '
                        'FROM ('
                            'SELECT '
                            'ext.mounted_on, '
                            'ext.fsize, '
                            'ext.used, '
                            'ext.Avail, '
                            'ext.use_pcent, '
                            'df.tablespace_name, '
                            'ROW_NUMBER() OVER ('
                                'PARTITION BY df.file_name '
                                'ORDER BY LENGTH(ext.mounted_on) DESC'
                            ') as rn '
                            'FROM dev.filesystem ext '
                            'JOIN dba_data_files df ON df.file_name LIKE \'%\' || ext.mounted_on || \'%\' '
                            'WHERE ext.mounted_on != \'/\''
                            'AND df.tablespace_name= :tspace '
                            'AND df.autoextensible= :xtensible '
                        ') subquery '
                        'WHERE rn = 1 '
                        'GROUP BY tablespace_name, mounted_on, fsize, used, Avail, use_pcent '
                        'ORDER BY tablespace_name'
                    )

                try:
                    # query to find if space exists on OS mount points
                    # it is good coding practice to use param binding in order to avoid SQL injection
                    # we bind tablespace to tspace and NO to xtensible here
                    cursor.execute(sql, {'tspace': tablespace, 'xtensible': 'NO'})
                except Exception as e:
                    logger.exception(f"Mountpoint verification query failed : {str(e)}")
                    return
                

                # Calculate the additional space needed in megabytes to bring it level to required_min_free_pct
                # and add 10% to the required_min_free_pct as a headroom
                required_min_free_pct = check_value

                # Variables
                # tspace_size_mb: Current total size of the tablespace in MB
                # used_space_mb: Current used space in MB
                # required_min_free_pct: Required minimum free percentage
                # pct_free: Current percentage of free space in the tablespace

                # Calculate the total required tablespace size to ensure required_min_free_pct free space
                used_space_mb= tspace_size - free
                total_required_space_mb = used_space_mb / (1 - required_min_free_pct / 100)

                # Calculate the raw additional space needed to reach the required free space
                raw_additional_space_needed_mb = total_required_space_mb - tspace_size

                # Ensure that the additional space is not negative
                additional_space_needed_mb = max(0, raw_additional_space_needed_mb)

                # Add 20% of the additional space needed as a headroom.
                total_additional_space_needed_with_headroom_mb = additional_space_needed_mb * 1.2

                # Assign the calculated value to self.additional_space_needed_mb
                self.additional_space_needed_mb = total_additional_space_needed_with_headroom_mb

                # Initialize variables to keep track of the remaining space needed and allocations
                self.remaining_space_needed = self.additional_space_needed_mb

                
                mpoints_big_enough = {}  # New dictionary for storing OS mount points big enough to contain additional_space_needed_mb for a tablespace
                one_to_many_mpoints = {}  # for storing OS Mount Points that map to more than one tablespace

                for row in cursor:
                    mpoint = row[0]
                    mpoint_size = self.convert_to_megabytes(row[1])
                    mpoint_used = self.convert_to_megabytes(row[2])
                    mpoint_avail = self.convert_to_megabytes(row[3])  # Convert to megabytes
                    mpoint_use_pcent = row[4]
                    tspace_name = row[5]     

                    # Check if there is no more space needed
                    if self.remaining_space_needed <= 0:
                        break  # Exit the loop as no more space allocation is needed

                    max_allowable_space = mpoint_size * 0.95 - (mpoint_size - mpoint_avail) #logic to allow not more than 95% filesystem/mount_point utilisation
                    # Check if max_allowable_space is non-negative
                    # Any value less than zero i.e. negative, indicates an existing utilisation of Mount Point beyond 95%
                    if max_allowable_space >= 0:

                        logger.info(f"Max allowable space for mpoint {mpoint} is {max_allowable_space}")

                        # Calculate the total space if additional space is added
                        #total_space_if_added = mpoint_used + self.additional_space_needed_mb

                        if self.additional_space_needed_mb <= max_allowable_space:
                            entry_key = mpoint
                            entry1 = {
                                'tablespace': tspace_name,
                                'mountpoint': mpoint,
                                'available_space': mpoint_avail,
                            }
                            # Store the entry in the mpoints_big_enough list
                            mpoints_big_enough[entry_key] = entry1

                            
                            # Check if mpoints_big_enough has entries
                            if len(mpoints_big_enough) > 0:
                                for key, entry1 in mpoints_big_enough.items():                                
                                    logger.info(
                                        f"Tablespace {tablespace} requires {self.additional_space_needed_mb} MB of more space, "
                                        f"and there is more than enough available space on mount point {entry1['mountpoint']} "
                                    )
                                
                                    mount_point=entry1['mountpoint']
                                    while self.remaining_space_needed > 0:
                                        space_to_allocate = min(self.remaining_space_needed, max_allowable_space)
                                        new_datafile=self.generate_new_datafile_name(tablespace, mount_point, self.generated_filenames)                                
                                        if new_datafile == None:
                                            break
                                        size_mb = min(math.ceil(self.remaining_space_needed), self.MAX_FILE_SIZE_MB)
                                        if self.MAX_FILE_SIZE_MB < max_allowable_space :
                                            #then we override size_mb and make size_mb same size as self.MAX_FILE_SIZE_MB
                                            size_mb= self.MAX_FILE_SIZE_MB
                                            space_to_allocate=size_mb
                                        else:
                                            #then we override size_mb and make size_mb same size as max_allowable_space
                                            size_mb=math.ceil(max_allowable_space)
                                            space_to_allocate=size_mb

                                        #print(f"space to be created is: {size_mb}")
                                        if self.create_data_file(tablespace, new_datafile, size_mb):
                                            # Update the remaining space needed
                                            self.remaining_space_needed -= size_mb
                                            
                                            # updates the dictionary (self.space_allocations) that keeps track of how much space has been allocated on each mount point.
                                            self.space_allocations[mount_point] = self.space_allocations.get(mount_point, 0) + space_to_allocate
            else:
                self.add_asm_datafiles( db, tablespace, free, pct_free, check_value, tspace_size)                               
                    
        if self.remaining_space_needed > 0: #for space requirements greater than a single mount , we use this call to spread it out across mulitiple mpoint.
            self.process_datafile_mpoints(tablespace)
        else:
            for mpoint, allocated_space in self.space_allocations.items():
                logger.info(f"Successfully allocated {allocated_space} MB across mount points.")
                logger.info(f"{allocated_space} MB allocated to mount point {mpoint}")

        cursor.close()
    
    def add_asm_datafiles(self, db, tablespace, free, pct_free, check_value, tspace_size):
        try:
            cursor = self.connection.cursor()
            cursor.arraysize = 1000
        except Exception as e:
            logger.exception(f"Catching function add_asm_datafiles cursor error: {str(e)}")
        sql=(
            'select name,state,type,total_mb,free_mb,usable_file_mb '
            'from v$asm_diskgroup '
            'where name like \'%DATA%\''
        )
        cursor.execute(sql) 
        # Calculate the additional space needed in megabytes to bring it level to required_min_free_pct
        # and add 20% to the required_min_free_pct as a headroom
        required_min_free_pct = check_value
        # Calculate the total required tablespace size to ensure required_min_free_pct free space
        used_space_mb= tspace_size - free
        total_required_space_mb = used_space_mb / (1 - required_min_free_pct / 100)

        # Calculate the raw additional space needed to reach the required free space
        raw_additional_space_needed_mb = total_required_space_mb - tspace_size

        # Ensure that the additional space is not negative
        additional_space_needed_mb = max(0, raw_additional_space_needed_mb)

        # Add 20% of the additional space needed as a headroom.
        total_additional_space_needed_with_headroom_mb = additional_space_needed_mb * 1.2

        # Assign the calculated value to self.additional_space_needed_mb
        self.additional_space_needed_mb = total_additional_space_needed_with_headroom_mb

        # Initialize variables to keep track of the remaining space needed and allocations
        self.remaining_space_needed = self.additional_space_needed_mb

        for row in cursor:
                    dg_name = row[0]
                    dg_state = row[1]
                    dg_type = row[2]
                    dg_size = row[3]
                    dg_free = row[4]
                    dg_usable_space = row[5]

                    # Check if there is no more space needed
                    if self.remaining_space_needed <= 0:
                        break  # Exit the loop as no more space allocation is needed
                    
                    max_allowable_space = dg_size * 0.95 - (dg_size - dg_usable_space) #logic to allow not more than 95% diskgroup utilisation

                    # Check if max_allowable_space is non-negative
                    # Any value less than zero i.e. negative, indicates an existing utilisation of diskgroup beyond 95%   
                    if max_allowable_space >= 0:

                        logger.info(f"Max allowable space for diskgroup {dg_name} is {max_allowable_space} MB")

                        if self.additional_space_needed_mb <= max_allowable_space:
                            logger.info(
                                        f"Tablespace {tablespace} requires {self.additional_space_needed_mb} MB of more space, "
                                        f"and there is more than enough available space on diskgroup {dg_name} "
                                    )
                            while self.remaining_space_needed > 0:
                                        space_to_allocate = min(self.remaining_space_needed, max_allowable_space)                                                                     
                                        size_mb = min(math.ceil(self.remaining_space_needed), self.MAX_FILE_SIZE_MB)
                                        
                                        if self.MAX_FILE_SIZE_MB < max_allowable_space :
                                            #then we override size_mb and make size_mb same size as self.MAX_FILE_SIZE_MB
                                            size_mb= self.MAX_FILE_SIZE_MB
                                            space_to_allocate=size_mb
                                        else:
                                            #then we override size_mb and make size_mb same size as max_allowable_space
                                            size_mb=math.ceil(max_allowable_space)
                                            space_to_allocate=size_mb
                                            
                                        sql_command = f"""alter tablespace {tablespace} add datafile '+{dg_name}' size {space_to_allocate}M"""

                                        if self.create_asm_data_file(sql_command, tablespace):                                       
                                            # Update the remaining space needed
                                            self.remaining_space_needed -= size_mb

        if self.remaining_space_needed > 0: #This might be useful for allocating datafiles into another diskgroup that become candidates
            logger.info(f"Tablespace {tablespace} still has {self.remaining_space_needed}MB remaining to be allocated to a diskgroup")
        else:
            logger.info(f"Successfully allocated {space_to_allocate} MB to diskgroup {dg_name}.")

        cursor.close()
                                            
    def create_asm_data_file(self,sql_command,tablespace):
        try:
            cursor = self.connection.cursor()
            cursor.arraysize = 1000
            cursor.execute(sql_command)
            cursor.close
            return True
        except Exception as e:
            logger.exception(f"Creating ASM datafile in tablespace {tablespace} failed! : {str(e)}")
            return False     
        
    
    def adjust_filepath(self, file_path_without_number, tablespace):
        last_segment = os.path.basename(file_path_without_number)
        if last_segment != tablespace:
            file_path_without_number = os.path.join(os.path.dirname(file_path_without_number), tablespace.lower()) #if base name is not tablespace name then replace it with tablespace name
            file_path_without_number = file_path_without_number.replace('\\', '/') #because the jenkins nodes are a windows OS,  the system introduces the backward slash in the filepath so we convert it back to Nix slash

        return file_path_without_number
    
    def generate_new_datafile_name(self, tablespace, mount_point, generated_filenames):
        # Define the SQL queries
        sql_query1 = """
            SELECT file_name 
            FROM dba_data_files 
            WHERE tablespace_name= :tspace
        """
        sql_query2 = """
            SELECT file_name 
            FROM dba_data_files 
            WHERE file_name LIKE :file_like
        """
        try:
            cursor = self.connection.cursor()
            cursor.arraysize = 1000
        except Exception as e:
            logger.exception(f"Establishing cursor for New Datafile name failed: {str(e)}")
        
        try:
            # Execute the SQL query to be used in finding the max numeric for the datafiles in the tablespace
            cursor.execute(sql_query1, tspace=tablespace)    
            existing_filenames = [row[0] for row in cursor] #we dump that data into a list
            #execute the query to be used in selecting the filesystem path for the eligible mount points
            cursor.execute(sql_query2, file_like=f'%{mount_point}%')   
            db_filenames = [row[0] for row in cursor] #we dump that data into a list
            max_number = 0
            file_path_without_number = None
            pattern = re.compile(r"(.+\/)([^\/]+?)(?:_(\d+))?\.dbf$")
            cnt=len(existing_filenames)
            # List to store extracted numeric suffix parts of the datafiles
            numeric_parts = []
            if cnt != 0: # debugging relic, this one.
                for file_name in existing_filenames:
                    match = pattern.match(file_name)
                    if match:
                        num_part = match.group(3)
                        if num_part:                            
                            numeric_parts.append(int(num_part)) #we add all the numeric suffixes to this list so we can perform MAX operation on it
                        else: #Here we take care of situations where no num_part exits for example filepath/demo.dbf
                            num_part=0 # Default value if num_part is missing.
                            numeric_parts.append(int(num_part))  
                            num_part='0' #we change value to arbitrary string value of '0' so it can pass the test "if num_part and num_part.isdigit():" 

                if num_part and num_part.isdigit():              
                    max_number = max(numeric_parts, default=0) 
                    new_num_part = max_number + 1            
                    if db_filenames: #list for filesystems eligible to receive a new datafile
                        file_name=db_filenames[0] #access the first record , no need to iterate through the whole lot
                        match = pattern.match(file_name)
                        if match:       
                            file_path = match.group(1)
                            base_name = match.group(2).lower()
                            file_path_without_number = os.path.join(file_path, base_name)
                            # Ensure consistent path separators
                            # Okay, my laptop was/is my test lab and being a WIndows OS laptop,  it always messed up the slashes so i had to force it.
                            file_path_without_number = file_path_without_number.replace('\\', '/') if os.sep == '\\' else file_path_without_number          

            if file_path_without_number is not None:                              
                # Adjust base_name part of the variable based on tablespace string
                file_path_without_number = self.adjust_filepath(file_path_without_number, tablespace)
                new_num_part= "00" + str(new_num_part) #the integer function used in appending to the list , strips off any leading zeros; so we convert to string and append back the two leading zeros
                new_filename = f"{file_path_without_number}_{str(new_num_part)}.dbf" 
                
                counter=1 #we initialise a counter needed for ensuring we don't get trapped in an indefinite loop as this is hypothetically possible without a counter here.
                while new_filename in generated_filenames and counter < 10:
                    new_num_part = int(new_num_part)  # Strip leading zeros by converting to an integer. This works as a reset button so we don't append endless zeros as it cycles through the loop.
                    new_num_part += 1
                    new_num_part= "00" + str(new_num_part)
                    new_filename = f"{file_path_without_number}_{str(new_num_part)}.dbf"
                    counter += 1        
                # Add the new filename to the set
                self.generated_filenames.add(new_filename)

                return new_filename
            else:
                logger.info(f"Unable to match any existing datafile in the mount point {mount_point}")
                return None
            cursor.close()
        except Exception as e:
              logger.exception(f"New Datafile name generation failed:{str(e)}")
              return None

    def create_data_file(self, tablespace, new_datafile, size_mb):
        sql_command = f"ALTER TABLESPACE {tablespace} ADD DATAFILE '{new_datafile}' SIZE {size_mb}M"
        try:
            cursor = self.connection.cursor()
            cursor.arraysize = 1000
            cursor.execute(sql_command)
            logger.info(f"New datafile {new_datafile} successfully added to tablespace {tablespace}")
            cursor.close
            return True
                
        except Exception as e:
            logger.exception(f"New Datafile addition to tablespace {tablespace} failed:{str(e)}")
            return False

    def split_host_port(self,dns,db):
        # Define a case-insensitive regular expression pattern to match HOST
        host_pattern = r"HOST=([a-zA-Z0-9.-]+)"
        
        # Use re.findall to find all case-insensitive HOST matches for HOST in the DNS string
        host_matches = re.findall(host_pattern, dns, re.IGNORECASE)
        # Use re.search with re.IGNORECASE to find the first case-insensitive HOST match
        #host_match = re.search(host_pattern, dns, re.IGNORECASE)

        if len(host_matches) > 1:
            logger.info(f"Hosts found in string, {host_matches}")
            # Extract each HOST value from the string
            for h in range(len(host_matches)):
                self.host_val = host_matches[h]
                logger.info(f"Testing connection for HOST:, {self.host_val}")
                
                # Define a case-insensitive regular expression pattern to match PORT
                port_pattern = r"PORT=(\d+)"
                # Use re.search with re.IGNORECASE to find the first case-insensitive PORT match
                port_match = re.search(port_pattern, dns, re.IGNORECASE)

                if port_match:
                    # Extract the PORT value from the match
                    self.port_val = port_match.group(1)
                    logger.info(f"PORT Value:, {self.port_val}")
                    self.db_conn(db)                    
                else:
                    logger.info(f"PORT not found in the connection string.")
                    
        else:
            logger.info(f"No multiple hosts found in conn string")
            self.host_val=host_matches[0]
            # Define a case-insensitive regular expression pattern to match PORT
            port_pattern = r"PORT=(\d+)"
            # Use re.search with re.IGNORECASE to find the first case-insensitive PORT match
            port_match = re.search(port_pattern, dns, re.IGNORECASE)

            if port_match:
                # Extract the PORT value from the match
                self.port_val = port_match.group(1)
                logger.info(f"PORT Value:, {self.port_val}")
                logger.info(f"Conn details is : , {self.connection}")
                self.db_conn(db)
                
            else:
                logger.info(f"PORT not found in the connection string.")
               
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
