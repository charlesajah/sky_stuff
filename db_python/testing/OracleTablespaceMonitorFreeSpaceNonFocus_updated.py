
import oracledb
import sys
import os
import platform
import re

class OracleTablespaceMonitorFreeSpaceNonFocus:
    QUERY_TIMEOUT = 30
    LOGIN_TIMEOUT = 30

    def __init__(self):
        self.min_percent = 3.0
        self.connection = None
        self.additional_space_needed_mb = None
        self.remaining_space_needed = None
        self.space_allocations = {}  # Dictionary to keep track of allocations
        self.tablespace_data = {}  # Dictionary to store all tablespace OS mount point data

    # ... [other methods and code] ...

    def process_datafile_mpoints(self, tablespace, check_value, tspace_size):
        # Calculate the additional space needed in megabytes
        self.additional_space_needed_mb = max(0, (tspace_size * (check_value / 100) - tspace_size * (pct_free / 100))) + 0.1 * (tspace_size * (check_value / 100))
        self.remaining_space_needed = self.additional_space_needed_mb

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT fsize, used, avail, use_pcent, mounted_on FROM dev.filesystem WHERE mounted_on LIKE '%/ora/data%'")

            for row in cursor:
                mp_size = self.convert_to_megabytes(row[0])
                mp_used = self.convert_to_megabytes(row[1])
                mp_avail = self.convert_to_megabytes(row[2])
                os_mp = row[4]

                entry = {
                    'mountpoint': os_mp,
                    'available_space': mp_avail,
                    'mpoint_size': mp_size,
                    'mpoint_used': mp_used,
                }

                self.tablespace_data[os_mp] = entry

            cursor.close()
        except Exception as e:
            print(f"Error querying filesystem: {e}")
            return

        for mpoint, data in self.tablespace_data.items():
            max_allowable_space = data['mpoint_size'] * 0.95 - data['mpoint_used']
            if max_allowable_space > 0 and self.remaining_space_needed > 0:
                space_to_allocate = min(self.remaining_space_needed, max_allowable_space)
                self.remaining_space_needed -= space_to_allocate
                self.space_allocations[mpoint] = self.space_allocations.get(mpoint, 0) + space_to_allocate
                self.tablespace_data[mpoint]['available_space'] -= space_to_allocate

        if self.remaining_space_needed > 0:
            print(f"Unable to allocate the required {self.additional_space_needed_mb} MB; {self.remaining_space_needed} MB still needed.")
        else:
            print(f"Successfully allocated {self.additional_space_needed_mb} MB across mount points.")
            for mpoint, allocated_space in self.space_allocations.items():
                print(f"{allocated_space} MB allocated to mount point {mpoint}")

    # ... [other methods and code] ...

if __name__ == "__main__":
    oracle_monitor = OracleTablespaceMonitorFreeSpaceNonFocus()
    oracle_monitor.run()
