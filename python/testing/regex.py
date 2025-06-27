import re
import os

import math
# Testing the revised regex pattern with a specific filename
pattern = re.compile(r"(.+\/)([^\/]+?)(?:_(\d+))?\.dbf$")
filename = "/trcomdbn02/ora/data01/TCC021N/demo.dbf"

match = pattern.match(filename)
if match:
    file_path = match.group(1)
    base_name = match.group(2)
    num_part = match.group(3)
    if num_part:            
        print(f"Match found: file_path: {file_path}, base_name: {base_name}, num_part: {num_part}")
    else:
        num_part='0'
    #max_number = max(int(num_part), default=0) 
    if num_part and num_part.isdigit(): 
        new_num_part = int(num_part) + 1     
        print(f"new_num_part value is {new_num_part}")
    else:
        print("No show")
else:
    print("No match found.")





  



