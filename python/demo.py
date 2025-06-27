import oracledb
import os
import platform

d = None  # default suitable for Linux
if platform.system() == "Windows":
    d = r"C:\oracle\product\instantclient_19_20"
oracledb.init_oracle_client(lib_dir=d)

try:
    dsn="""(DESCRIPTION=
                                  (ADDRESS=(PROTOCOL=TCP)(HOST=onxissapdbn01)(PORT=1525))
                                  (ADDRESS=(PROTOCOL=TCP)(HOST=sceissapdbn01)(PORT=1525))
                                  (CONNECT_DATA=(SERVICE_NAME=ISSAPN01_PRI)))"""
    #connection = oracledb.connect(user="HP_DIAG", password='HP_DIAG_1234', dsn=dsn
    #connection = oracledb.connect(user="HP_DIAG",password='HP_DIAG_1234', host="ccapedbn01", service_name="CBP012N", port=1525)
    connection = oracledb.connect(user="HP_DIAG",password='HP_DIAG_1234', dsn="CBP012N",config_dir="C:\oracle\product\instantclient_19_20\network\admin\tnsnames.ora")
    print("Object type for connection is:", type(connection))
    print('Connected to Oracle database successfully.')
    print("Display connection variable :" , connection)
except Exception as e:
    # Handle any other unexpected exceptions.
    print('An error occurred:', e)
    print(oracledb)