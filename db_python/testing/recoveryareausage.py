import logging
logger = logging.getLogger(__name__)

class RecoveryAreaUsage:
    def __init__(self, File_Type, Percent_Space_Used, Percent_Space_Reclaimable):
        self.File_Type = File_Type
        self.Percent_Space_Used = Percent_Space_Used 
        self.Percent_Space_Reclaimable = Percent_Space_Reclaimable
        

    def get_file_type(self):
        return self.file_type

    def get_percent_space_used(self):
        return self.percent_space_used

    def get_Percent_Space_Reclaimable(self):
        return self.percent_space_reclaimable
    
    def config_hp_diag(self, db):
        
        # SQL to check if the function and types already exist
        sql_check = """
            SELECT object_name
            FROM dba_objects
            WHERE owner = 'HP_DIAG'
            AND object_name IN ('READ_FILE', 'FILE_LINE_ROW', 'FILE_LINE_TABLE')
            AND object_type IN ('FUNCTION', 'TYPE')
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
            #cursor = self.connection.cursor()
            cursor=db.cursor()
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
                
        except Exception as e:
            logger.exception(f"Error in creating HP_DIAG objects: {str(e)}")
        finally:
            cursor.close()  # Ensure the cursor is closed after operation

    def check_arch_filesystem(self, db):
        # Your SQL command
        read_file = "SELECT line_content FROM TABLE(hp_diag.read_file())"
        #logger.info(f"Did we venture into this method at all?")
        
        try:
            #cursor = self.connection.cursor()
            cursor=db.cursor()
            cursor.arraysize = 1000
            cursor.execute(read_file)
            results = cursor.fetchall()  # Fetch all results at once
            line_storage = {}
            logger.info(f"Show all {read_file}")

            for result in results:
                line = result[0]
                if 'Checking Directory' in line:
                    db_name = line.split("/")[-2]  # Extract the database name by splitting the string into sub-strings using the slash as a delimiter and subsequently selecting the second sub-string backwards
                    dname = db.get_name() # we fetch the global database being iterated by the process_database() method
                    if db_name.upper() == dname.upper(): # 
                        line_storage[db_name] = {'db_line': line, 'dir_line': None}
                        #we extract archivelog path
                        path_parts = line.split("'")
                        dir_line = path_parts[1]  # we extract the substring from the split which should contain the archivelog OS path
                        util_val = path_parts[2] # we extract the % utilisation for the filesystem
                        percent_util = util_val.split()[-3].strip('%')
                        
                        #logger.info(f"db_name is {db_name} and dname is {dname}")
                        
                        if  float(percent_util) < 90.0 : 
                            #logger.info(f"ArchiveLogDestination:{db_name} {dir_line}")
                            logger.info(f"_datapoint:ArchiveLogFilesystemSpace:{db_name} ArchiveLogDestination:{dir_line} %Utilisation:{percent_util}%")
                                

        except Exception as e:
            logger.exception(f"Check logfile system error occurred: {str(e)}")
        
        finally:
            cursor.close()
