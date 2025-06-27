#!/bin/ksh

# Run the df -h command and capture its output
df_output=$(df -h)

# Specify the CSV file path
csv_file_path='/apps/ora/home/df_h/df_output.csv'

# Process the output to ensure each line in the CSV has exactly six values
echo "$df_output" | awk '
BEGIN { OFS = "," }
NR == 1 {
    # Correctly format the header
    sub(/Mounted on/, "Mounted_On")
    gsub(/ +/, OFS)
    print
    next
}
{
    # Process each line
    for (i = 1; i <= NF; i++) {
        # Accumulate fields into current_line
        current_line = (current_line ? current_line OFS : "") $i
        if (++field_count == 6) {
            # Print the accumulated line after every 6 fields
            print current_line
            current_line = ""
            field_count = 0
        }
    }
}
END {
    # Print any remaining fields in the last line
    if (current_line != "") print current_line
}' > "$csv_file_path"

