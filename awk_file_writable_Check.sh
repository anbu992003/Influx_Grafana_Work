awk '{
  cmd = "test -w \"" $2 "\"";
  writable = system(cmd);
  if (writable == 0) {
    print $2 " is writable"
  } else {
    print $2 " is not writable"
  }
}' input_file






#!/bin/bash

# Function to check if file path is writable
check_writable() {
  local filepath="$1"
  if [ -w "$filepath" ]; then
    echo "yes"
  else
    echo "no"
  fi
}

export -f check_writable  # Export the function so it's available to awk

# Sample input file (replace with your actual file)
input_file="filepaths.txt"

awk '
  {
    cmd = "bash -c \"check_writable \047" $0 "\047\""
    cmd | getline is_writable
    close(cmd)
    print $0, is_writable
  }
' "$input_file"
