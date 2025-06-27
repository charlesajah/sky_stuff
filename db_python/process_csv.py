import csv

# Input and output file paths
#input_file_path = r'C:\testing\uprn\skyq.csv'
#output_file_path = r'C:\testing\uprn\skyq_291223.sql'

input_file_skyq = 'skyq.csv'
output_file_skyq = 'sky_q.sql'

input_file_skym = 'skym.csv'
output_file_skym = 'sky_m.sql'

input_file_skyplus = 'skyplus.csv'
output_file_skyplus= 'sky_plus.sql'

# Function to generate SQL statements for updating the 5th column for the first 10,000 records
def generate_sql_statements(input_path, output_path, num_records=10000):
    with open(input_path, 'r', newline='') as infile, open(output_path, 'w') as outfile:
        reader = csv.reader(infile)

        # Skip the empty line and the header
        next(reader)
        next(reader)

        # Write SQL statements to the output file
        for _ in range(num_records):
            try:
                row = next(reader)
                sql_statement = row[4]
                outfile.write(sql_statement + '\n')
            except StopIteration:
                break
        
        # Add a commit statement at the end
        outfile.write('COMMIT;\n')

# Call the function
generate_sql_statements(input_file_skyq, output_file_skyq)
generate_sql_statements(input_file_skym, output_file_skym)
generate_sql_statements(input_file_skyplus, output_file_skyplus)

