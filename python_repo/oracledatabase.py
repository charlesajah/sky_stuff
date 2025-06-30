from oracletablespace import OracleTablespace
import logging
logger = logging.getLogger(__name__)



class OracleDatabase(OracleTablespace):
    def __init__(self, dbName, tnsString, username, password, excludedTablespaces, percent):
        self.dbName = dbName
        self.tnsString = tnsString
        self.username = username
        self.password = password
        self.percent = percent
        self.excludedTablespaces = [OracleTablespace(ts) for ts in excludedTablespaces.split(":")]

    def get_name(self):
        return self.dbName

    def get_tns(self):
        return self.tnsString

    def get_username(self):
        return self.username

    def get_password(self):
        return self.password

    def get_excluded_tablespaces(self):
        return self.excludedTablespaces

    def get_percent(self):
        return self.percent

    def set_percent(self, percent):
        self.percent = percent

    def is_tablespace_excluded(self, search_tablespace):
        return any(tablespace.get_name() == search_tablespace for tablespace in self.excludedTablespaces)
