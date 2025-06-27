#!/bin/ksh

# Run the df -h command and capture its output
df_output=$(df -h)

# Specify the CSV file path
csv_file_path='/apps/ora/home/df_h/df_output.csv'

# Get the current timestamp
timestamp=$(date '+%Y-%m-%d %H:%M:%S')

# Extract and modify the header
header=$(echo "$df_output" | awk 'NR==1 {gsub(/Mounted on/, "Mounted_On"); print}' | tr -s ' ')
header="$header,timestamp"

# Write the modified header and data to a CSV file
echo "$header" > "$csv_file_path"
echo "$df_output" | awk -v ts="$timestamp" 'NR>1 {print $0, ts}' | tr -s ' ' | sed 's/ /,/g'|sed 's/,\([^,]*\)$/ \1/'  >> "$csv_file_path"

