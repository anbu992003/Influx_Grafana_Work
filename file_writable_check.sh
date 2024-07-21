if [ -w "/path/to/your/file" ]; then
  echo "File is writable"
else
  echo "File is not writable"
fi



file="/path/to/your/file"
if [ -w "$file" ]; then
  echo "$file is writable"
elif [ ! -f "$file" ]; then
  echo "$file does not exist"
else
  echo "$file exists but is not writable"
fi



#!/bin/bash

# Check if the file path is provided as an argument
if [ -z "$1" ]; then
  echo "Usage: $0 <filepath>"
  exit 1
fi

FILEPATH="$1"

# Check if the file path is writable
if [ -w "$FILEPATH" ]; then
  echo "The file path '$FILEPATH' is writable."
else
  echo "The file path '$FILEPATH' is not writable."
fi




