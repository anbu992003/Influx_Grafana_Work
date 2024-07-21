~$ find /mnt/c/Program\ Files/ -mindepth 1 -maxdepth 6 -type d >file_list.txt
find: ‘/mnt/c/Program Files/WindowsApps’: Permission denied

#!/bin/bash

# Define paths for output and error files
OUTPUT_FILE="subdirectories.txt"
ERROR_FILE="errors.txt"

# Clear previous content in output and error files
> "$OUTPUT_FILE"
> "$ERROR_FILE"

# Define the starting directory
START_DIR="$1"

# Check if the starting directory is provided
if [ -z "$START_DIR" ]; then
    echo "Usage: $0 <starting-directory>" | tee -a "$ERROR_FILE"
    exit 1
fi

# Check if the starting directory exists
if [ ! -d "$START_DIR" ]; then
    echo "Starting directory $START_DIR does not exist." | tee -a "$ERROR_FILE"
    exit 1
fi

# Find subdirectories and handle errors
find "$START_DIR" -type d 2>> "$ERROR_FILE" | tee -a "$OUTPUT_FILE"

# Check if the find command was successful
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "Error occurred while finding subdirectories." | tee -a "$ERROR_FILE"
fi

echo "Subdirectory listing has been saved to $OUTPUT_FILE."
echo "Errors (if any) have been saved to $ERROR_FILE."
