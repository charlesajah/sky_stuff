import argparse
import csv
import requests
import sys
import oracledb
from requests.exceptions import RequestException
from urllib.parse import urlparse
import json
from typing import List
from oracledatabase import OracleDatabase
from recoveryareausage import RecoveryAreaUsage
import logging

logger = logging.getLogger(__name__)
          

class Datapump(OracleDatabase):
    def __init__(self):
        self.query_file = None
        self.db_list_src = None
        self.datapump_type = ""
        self.use_model = False
        self.handle_tablespaces = True
        self.db_list = []
        self.query = ""
        self.min_percent = 5.0
        self.environment = ""
        self.databases = []
        self.influx_url = ""
        self.influx_org = ""
        self.influx_bucket = ""
        self.influx_token = ""
        self.recovery_area= []
        

    def get_db_list(self, args):
        if not self.use_model:
            return self.get_db_list_from_file(self.db_list_src)
        else:
            return self.get_db_list_from_model(self.db_list_src)

    def get_db_list_from_file(self, filename):
        try:
            with open(filename, 'r') as file:
                reader = csv.reader(file)
                for row in reader:
                    if not row or row[0].startswith('#'):
                        continue  # Skip empty lines and lines starting with '#'
                    if len(row) == 6 or ',' in row[0]:
                        self.databases.append(OracleDatabase(row[0], row[1], row[2], row[3], row[4], float(row[5])))
                    else:
                        self.databases.append(OracleDatabase(row[0], row[1], row[2], row[3], row[4], -1.0))
            return True
        except Exception as e:
            logger.exception(f"ERROR: Failed to get the list of databases to process from '{filename}': {e}")
            return False


    def get_db_list_from_model(self, url):
        try:
            db_array = self.get_model_databases(url)
            for db_obj in db_array:
                if 'tns' in db_obj and 'sid' in db_obj:
                    tns = db_obj['tns']
                    sid = db_obj['sid']
                    self.databases.append(OracleDatabase(sid, tns, "HP_DIAG", "HP_DIAG_1234", "UNDOTBS", -1.0))
            return True
        except Exception as e:
            logger.exception(f"ERROR: Problems parsing model response: {e}")
            return False

    def get_model_databases(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except RequestException as e:
            logger.exception(f"ERROR: Could not get JSON from Environment Model @ {url}: {e}")
            return []
    def get_sql(self, query_file):
        if query_file:
           try:
               with open(query_file, 'r') as file:
                   self.query = file.read()
                   return True
           except Exception as e:
                    logger.exception(f"ERROR: Failed to read SQL from '{query_file}': {e}")
                    return False
        else:
            logger.info("Query file has not been read properly")
       

    def parse_command_line(self, args):
        parser = argparse.ArgumentParser(description="Export or Import SQL plan baselines and/or SQL Profiles.")
        parser.add_argument("-s", type=str, help="Path to file containing SQL to execute.")
        parser.add_argument("-d", type=str, required=True, help="Path to file containing list of database TNS string to process.")
        parser.add_argument("-m", type=str, help="URL that returns a list of databases from the environment model.")
        parser.add_argument("-n", action="store_false", help="Disable tablespace reporting.")
        parser.add_argument("-p", type=float, help="Minimum percent free")
        parser.add_argument("-a", type=str, required=True, choices=['export','import'], help="Define whether you want to export or import SQL baselines and profiles")
        parser.add_argument("-e", type=str, help="The environment being processed.")
        parser.add_argument("-i", type=str, help="Influx DB URL")
        parser.add_argument("-b", type=str, help="Influx DB bucket")
        parser.add_argument("-o", type=str, help="Influx DB organization")
        parser.add_argument("-t", type=str, help="Influx DB token")
        
        args = parser.parse_args(args)

        if args.a and (args.s or args.p):
                parser.error("Argument -a cannot be used with -s or -p")
        elif not args.a and (not args.s or not args.d):
            parser.error("When -a is not provided, both -s and -d must be specified")
            
        if args.s and args.d and args.p:
            self.query_file = args.s
            self.db_list_src = args.d
            self.min_percent = args.p
            self.use_model = False
            return args
        elif args.a and args.d:
            self.datapump_type = self.datapump_type
            self.db_list_src = args.d
            return args
        elif args.s and args.m:
            self.query_file = args.s
            self.db_list_src = args.m
            self.use_model = True
            return args
        else:
            logger.error("ERROR: Must specify a query file and either a database list or a URL to a model that produces a database list.")
            return False
        
        #if args.p:
        #   self.min_percent = args.p
        #  logger.info(f"Argument supplied for -p is "{self.min_percent})
        
        #args = parser.parse_args(args)
        #return args


    def populate(self):
        args = self.parse_command_line(sys.argv[1:])
        logger.debug(f"Parsed arguments: {args}")
        logger.debug(f"query_file: {self.query_file}")
        logger.debug(f"db_list_src: {self.db_list_src}")
        logger.debug(f"use_model: {self.use_model}")
        logger.debug(f"min_percent: {self.min_percent}")

        if not self.query_file and not self.db_list_src:
            logger.error("No query_file or db_list_src provided.")
            return False

        if self.query_file and not self.get_sql(self.query_file):
            logger.error("Failed to read SQL file.")
            return False

        if self.db_list_src and not self.get_db_list(self.db_list_src):
            logger.error("Failed to get database list.")
            return False

        return True

    def process_database(self, db, handle_tablespaces):
        pass

if __name__ == "__main__":
    dpump = Datapump() 
    dpump.populate()
