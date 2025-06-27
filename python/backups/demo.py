import oracledb

try:
    connection = oracledb.connect(user="HP_DIAG",password='HP_DIAG_1234',host="chorddbptt", port=1521, service_name="CHORDO")
    print('Connected to Oracle database successfully.')
except Exception as e:
    # Handle any other unexpected exceptions.
    print('An error occurred:', e)
    print(oracledb)