import re

def split_conn_string(dns):
    #print("Testing if split_conn_string was entered at all", dns)

    # Define a regular expression pattern to match HOST
    host_pattern = r"HOST=([a-zA-Z0-9.-]+)"

    # Use re.findall to find all matches for HOST in the DNS string
    host_matches = re.findall(host_pattern, dns, re.IGNORECASE)

    # Check if any host matches were found
    if host_matches:
        # Return the list of host names
        print( "There are ",len(host_matches))
        return host_matches
    else:
        print("No HOST matches found in the sample data.")
        return []

dns = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=onxissapdbn01)(PORT=1525))(ADDRESS=(PROTOCOL=TCP)(HOST=sceissapdbn01)(PORT=1525))(CONNECT_DATA=(SERVICE_NAME=ISSAPN01_PRI)))"
host_names = split_conn_string(dns)
print("Host Names:", host_names)
