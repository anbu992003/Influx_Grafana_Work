#!/bin/bash
SCRIPT_PATH=$(realpath "${BASH_SOURCE[0]}")
echo "Script path: $SCRIPT_PATH"

#!/bin/bash
SCRIPT_PATH=$(realpath "$0")
echo "Script path: $SCRIPT_PATH"


	
#!/bin/bash

# File descriptors for clarity
exec 3>&1  # Save stdout to file descriptor 3
exec 4>&2  # Save stderr to file descriptor 4

# Redirect stderr to error file
exec 2> error.log

# Function to log debug messages
log_debug() {
  echo "$(date +"%Y-%m-%d %H:%M:%S") - DEBUG: $@" >> debug.log
}

# Function to send InfluxDB messages
send_influxdb_data() {
  # Replace with your actual InfluxDB data generation logic
  influxdb_data="some_influxdb_data"
  echo "$influxdb_data" >&3  # Send to original stdout
}

# Example usage
log_debug "This is a debug message"
send_influxdb_data
echo "This is an error message" >&2  # Forcefully send to stderr

# Restore original stdout and stderr
exec 2>&4
exec 1>&3







#!/bin/bash

# Define log files
DEBUG_LOG="debug.log"
ERROR_LOG="error.log"

# Redirect general output (stdout) to debug.log
exec 3>&1 1>>"$DEBUG_LOG"

# Redirect errors (stderr) to error.log
exec 2>>"$ERROR_LOG"

# Function to simulate InfluxDB protocol message handling
process_influxdb_message() {
    local message="$1"
    
    # Check if the message contains the InfluxDB protocol (e.g., "influxdb")
    if echo "$message" | grep -q "influxdb"; then
        # Print to stdout
        echo "$message" >&3
    else
        # Print to general output (debug.log)
        echo "$message"
    fi
}

# Example usage:

# General debug message (goes to debug.log)
echo "This is a general debug message."

# Simulate processing an InfluxDB protocol message (goes to stdout)
process_influxdb_message "influxdb: This is an InfluxDB protocol message."

# Error message (goes to error.log)
echo "This is an error message." >&2

# Another general message (goes to debug.log)
echo "Another general debug message."

# Another InfluxDB message (goes to stdout)
process_influxdb_message "influxdb: Another InfluxDB protocol message."

ssh user@remote-host 'bash -s' < commands.txt



import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

def stat_directory(directory):
    try:
        output = subprocess.check_output(['stat', directory], stderr=subprocess.STDOUT)
        return output.decode('utf-8')
    except subprocess.CalledProcessError as e:
        return f"Error with {directory}: {e.output.decode('utf-8')}"

def main():
    directories = subprocess.check_output(['find', '/path/to/search', '-type', 'd']).decode('utf-8').splitlines()
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(stat_directory, d) for d in directories]
        for future in as_completed(futures):
            print(future.result())

if __name__ == "__main__":
    main()
