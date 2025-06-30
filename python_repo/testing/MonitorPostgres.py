import psycopg2
from psycopg2 import OperationalError
from custom_logger import CustomLogger
import logging

# Instantiate the logger
logger_instance = CustomLogger(log_file='MonitorPostgres.log', log_level=logging.DEBUG, console_level=logging.INFO)
logger = logger_instance.get_logger()

def check_instance_role(host, port, user, password):
    try:
        # Connect to the PostgreSQL instance
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database="postgres"
        )
        connection.autocommit = True
        cursor = connection.cursor()

        # Check if the instance is in recovery mode (which would mean it's a standby)
        cursor.execute("SELECT pg_is_in_recovery();")
        is_in_recovery = cursor.fetchone()[0]

        # Determine if this is the primary or standby instance
        if is_in_recovery == False:
            #print(f"The content of is_in_recovery is {is_in_recovery}")
            role = "Primary"
            if role:
                return role
        elif is_in_recovery == True:
            #print(f"The content of is_in_recovery is {is_in_recovery}")
            role = "Standby"
            if role:
                return role
        else:
            role=None

        cursor.close()
        connection.close()
        
        
    except OperationalError as e:
        logger.error(f"Error: Could not connect to the PostgreSQL instance on {host}. Details: {e}")
        return None

def check_database_status(host, port, user, password, dbname):
    try:
        # Attempt to connect to the specified database
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=dbname
        )
        connection.autocommit = True
        connection.close()
        return True
    except OperationalError:
        return False

def check_postgres_instance(instance_name, host, port, user, password):
    try:
        # Connect to the PostgreSQL server to retrieve the list of databases
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database="postgres"
        )
        connection.autocommit = True

        # Create a cursor object
        cursor = connection.cursor()

        # Execute a command to list all databases
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = cursor.fetchall()

        logger.info(f"{instance_name} PostgreSQL instance  is running on host {host}.")
        logger.info("Checking the status of each database:")
        
        # Check the status of each database
        for db in databases:
            db_name = db[0]
            if check_database_status(host, port, user, password, db_name):
                logger.info(f"- {db_name}: Running")
            else:
               logger.info(f"- {db_name}: Down")

        if instance_name == 'Primary':
            check_vital_signs(instance_name, host, port, user, password)            

        # Close the cursor and connection
        cursor.close()
        connection.close()
    except OperationalError as e:
        logger.error(f"Error: Could not connect to the {instance_name} PostgreSQL instance on {host}. Details: {e}")

def check_vital_signs(instance_name, host, port, user, password):
    try:
        # Connect to the PostgreSQL server to retrieve the list of databases
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database="postgres"
        )
        connection.autocommit = True

        # Create a cursor object
        cursor = connection.cursor()

        # Execute a command to check the difference between the oldest TX and current TX

        query1 = """
            SELECT
                pg_current_xact_id()::text::bigint AS current_xid,
                s.oldest_xid::text::bigint AS oldest_xid,
                pg_current_xact_id()::text::bigint - s.oldest_xid::text::bigint AS xid_growth
            FROM
                (SELECT
                    datname,
                    datfrozenxid AS oldest_xid,
                    age(datfrozenxid) AS age_oldest_xid
                FROM
                    pg_database) s
            WHERE
                datname = current_database();
            """
        cursor.execute(query1)
        # Fetch the result
        result1 = cursor.fetchone()
        current_xid, oldest_xid, xid_growth = result1

        # Print the results
        logger.info("")
        logger.info(f"***XID Growth Report***")
        logger.info(f"Current XID for database is : {current_xid}")
        logger.info(f"Oldest XID for database is: {oldest_xid}")
        logger.info(f"XID Growth for database is: {xid_growth}")

        if xid_growth > 1995000000 :
            logger.info(f"The current Transaction ID of the instance at {xid_growth} is nearing a critical threshold of 2 billion!")
            logger.info("Please confirm if the WAL streaming to the standby is working fine!")


        ##section for checking inactive replication slots
        query2 = """select count(*) from pg_replication_slots where not active;"""  
        cursor.execute(query2) 
        result2 = cursor.fetchone()
        if result2 is not None and result2[0] > 0:
            print(f"There are {result2[0]} replication slots that need urgent attention")
        

        #write lag (no. of bytes sent but not written to client’s disk)
        #flush lag (no. of bytes written but not flushed to client’s disk) 
        #replay lag (no. of bytes flushed but not replayed into the client’s database files) for each active WAL sender
        #section for monitoring replication lags for physical replications
        query3 = """ SELECT to_char(date_trunc('second', write_lag), 'HH24:MI:SS') as write_lag,
                    to_char(date_trunc('second', flush_lag), 'HH24:MI:SS') as flush_lag, 
                    to_char(date_trunc('second', replay_lag), 'HH24:MI:SS') as replay_lag FROM pg_stat_replication;"""
        cursor.execute(query3)
        result3 = cursor.fetchone()
        if result3 is not None:
            write_lag, flush_lag, replay_lag = result3
            if write_lag != None or flush_lag != None or replay_lag != None :
                logger.info("")
                logger.info("***WAL Log Streaming Report***")
                logger.info(f"write_lag on {host} is: {write_lag}. flush_lag on {host}is: {flush_lag}. replay_lag on standby instance is: {replay_lag}.")
            else:
                logger.info("")
                logger.info("***WAL Log Streaming Report***")
                logger.info("The standby instance on host {host} is all caught up!")

        
        query4 = """ SELECT
                        COALESCE(blockingl.relation::regclass::text, blockingl.locktype) AS locked_item,
                        to_char(date_trunc( 'second',now() - blockeda.query_start), 'HH24:MI:SS') AS waiting_duration,  -- Truncate to remove subseconds
                        blockeda.pid AS blocked_pid,
                        blockeda.query AS blocked_query,
                        blockedl.mode AS blocked_mode,
                        blockinga.pid AS blocking_pid,
                        blockinga.query AS blocking_query,
                        blockingl.mode AS blocking_mode,
                        db.datname AS database_name  -- Add database name to the query
                    FROM pg_catalog.pg_locks blockedl
                    JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
                    JOIN pg_catalog.pg_locks blockingl ON (
                        ( (blockingl.transactionid = blockedl.transactionid) OR
                            (blockingl.relation = blockedl.relation AND blockingl.locktype = blockedl.locktype)
                        ) AND blockedl.pid != blockingl.pid
                        )
                    JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
                        AND blockinga.datid = blockeda.datid
                    JOIN pg_database db ON db.oid = blockeda.datid  -- Join to get the database name
                        WHERE NOT blockedl.granted;
                 """
        
        
        cursor.execute(query4)
        result4 = cursor.fetchall()
        if not result4:
            logger.info("")
            logger.info("***Databases Locks report***")
            logger.info("No locks found")
        else:
            lock_count = len(result4)
            for lock in result4:
                locked_item, waiting_duration, blocked_pid, blocked_query, blocked_mode, blocking_pid, blocking_query, blocking_mode, database_name = lock
                logger.info("")
                logger.info("***Databases Locks report***")
                logger.info(f"The PID {blocked_pid} on database {database_name} and host {host} is waiting for a lock in {blocked_mode} mode being held by pid {blocking_pid} since {waiting_duration}. ago")

        query5="""select pid, to_char(date_trunc('second',now() - pg_stat_activity.query_start), 'HH24:MI:SS') AS duration, state, datname FROM pg_stat_activity WHERE (now() - pg_stat_activity.query_start) > interval '2 seconds';"""
        cursor.execute(query5)
        result5 = cursor.fetchall()
        if not result5:
            logger.info("")
            logger.info("***No long running queries up to 5 minutes***")
        else:
            logger.info("")
            logger.info("*** Long Running Queries Report***")
            for long_running_queries in result5:
                pid, duration, state, database_name = long_running_queries
                logger.info(f"The PID {pid} on database {database_name} and host {host} has been running for {duration}.")
        # Close the cursor and connection
        cursor.close()
        connection.close()

    except OperationalError as e:
        logger.error(f"Error: Could not connect to the {instance_name} PostgreSQL instance on {host}. Details: {e}")

if __name__ == "__main__":
    # Define the instances you want to check
    instances = [
        {"host": "ud-0010627", "port": "5432", "user": "nfttest", "password": "K0nGNfTtst2024"},
        {"host": "ud-0010628", "port": "5432", "user": "nfttest", "password": "K0nGNfTtst2024"}
    ]

    # Check each instance
    for instance in instances:
        host = instance["host"]
        port = instance["port"]
        user = instance["user"]
        password = instance["password"]

        role = check_instance_role(host, port, user, password)
        if role:
            logger.info(f"\nChecking {role} instance:")
            check_postgres_instance(role, host, port, user, password)
        else:
            logger.info(f"check_instance_role function failed to return a value")
