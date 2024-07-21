path doesnot exist
dir_path:

Wondershare/Wondershare\ Filmora\ (CPC)/Effect/opencl/lookup



Recursively list all files and directories within a given path.
For each file and directory, display:
File size
Full filepath
Permissions
Owner
Group
Last modified time
Last accessed time

find /path/to/your/directory -type f -exec stat -c '%s %n %A %U %G %y %x' {} \;
find /path/to/directory -exec ls -l --time=atime {} \;

find /mnt/c/Program\ Files/ -type f -exec stat -c '%s %n %A %U %G %y %x' {} \;
real    4m51.729s
user    1m11.606s
sys     0m33.442s
find /mnt/c/Program\ Files/ -exec ls -l --time=atime {} \;
real    4m51.729s
user    1m11.606s
sys     0m33.442s


find /path/to/directory -print0 | xargs -0 -n1 -P4 -I{} ls -l --time=atime {}
find /path/to/directory -print0 | xargs -0 -n1 -P4 -I{} stat -c '%s %n %A %U %G %y %x' {}

find /mnt/c/Program\ Files/ -print0 | xargs -0 -n1 -P4 -I{} ls -l --time=atime {}
real    3m19.797s
user    1m56.886s
sys     1m14.402s
find /mnt/c/Program\ Files/ -print0 | xargs -0 -n1 -P4 -I{} stat -c '%s %n %A %U %G %y %x' {}
real    1m34.237s
user    1m36.315s
sys     0m34.253s

>>61197 files


find /path/to/your/directory -type f -print0 | xargs -0 -P 0 stat -c '%s %n %A %U %G %y %x' 2>&1 | tee output.txt | grep -v 'No such file or directory' > success.txt 2> errors.txt
find /mnt/c/Program\ Files/ -type f -print0 | xargs -0 -P 0 stat -c '%s %n %A %U %G %y %x' 2>&1 | tee output.txt | grep -v 'No such file or directory' > success.txt 2> errors.txt
real    1m4.707s
user    0m3.181s
sys     0m35.160s
>>53609 files



#!/bin/bash

# Define paths
TARGET_DIR="/path/to/directory"
OUTPUT_FILE="output.txt"
ERROR_FILE="errors.txt"

# Ensure output files are empty
: > $OUTPUT_FILE
: > $ERROR_FILE

# Run the command
find "$TARGET_DIR" -print0 | parallel -0 -j $(nproc) 'stat -c "%s %n %A %U %G %y %x" "{}" >> $OUTPUT_FILE 2>> $ERROR_FILE'




change the below command output to line protocol format to insert into influxdb by adding the currenttimestamp at the end

find /mnt/c/Program\ Files/ -print0 | xargs -0 -n1 -P4 -I{} stat -c '%s %n %A %U %G %y %x' {}

>>ERROR
find /mnt/c/Program\ Files/ -print0 | xargs -0 -n1 -P4 -I{} sh -c 'stat -c "%s %n %A %U %G %y %x" "{}" | awk -v timestamp=$(date +%s%N) \'{print "filesystem_stat,size="$1",permissions="$3",user="$4",group="$5" file=\""gensub(/"/, "\\\\\"", "g", $2)"\",modified_time=\""gensub(/"/, "\\\\\"", "g", $6)"\",accessed_time=\""gensub(/"/, "\\\\\"", "g", $7)"\" "timestamp}\''

>>>WORKED
find /mnt/c/Program\ Files/ -print0 | xargs -0 -n1 -P4 -I{} sh -c 'stat -c "%s %n %A %U %G %y %x" "{}" | awk -v timestamp=$(date +%s%N) '\''{ gsub(/"/, "\\\\\"", $2); gsub(/"/, "\\\\\"", $6); gsub(/"/, "\\\\\"", $7); print "filesystem_stat,size="$1",permissions="$3",user="$4",group="$5" file=\"" $2 "\",modified_time=\"" $6 "\",accessed_time=\"" $7 "\" " timestamp }'\'' >> success.txt 2>> error.txt'

anbu992003@LAPTOP-ODISB5A1:~$ find /mnt/c/Program\ Files/ -print0 | xargs -0 -n1 -P4 -I{} sh -c 'stat -c "%s %n %A %U %G %y %x" "{}" | awk -v timestamp=$(date +%s%N) '\''{ gsub(/"/, "\\\\\"", $2); gsub(/"/, "\\\\\"", $6); gsub(/"/, "\\\\\"", $7); print "filesystem_stat,size="$1",permissions="$3",user="$4",group="$5" file=\"" $2 "\",modified_time=\"" $6 "\",accessed_time=\"" $7 "\" " timestamp }'\'' >> success.txt 2>> error.txt'
stat: cannot stat '/mnt/c/Program Files/Microsoft Visual Studio/2022/Community/Common7/IDE/CommonExtensions/Microsoft/TextMate/Starterkit/Extensions/php/Snippets/return ;.tmSnippet': No such file or directory
find: ‘/mnt/c/Program Files/WindowsApps’: Permission denied
>>114805 files



anbu992003@LAPTOP-ODISB5A1:~$ find /mnt/c/Program\ Files/ -print0 2>error.txt | xargs -0 -n1 -P4 -I{} sh -c 'stat -c "%s %n %A %U %G %y %x" "{}" | awk -v timestamp=$(date +%s%N) '\''{ gsub(/"/, "\\\\\"", $2); gsub(/"/, "\\\\\"", $6); gsub(/"/, "\\\\\"", $7); print "filesystem_stat,size="$1",permissions="$3",user="$4",group="$5" file=\"" $2 "\",modified_time=\"" $6 "\",accessed_time=\"" $7 "\" " timestamp }'\'' ' > success.txt 2>> error.txt
>>61196

>>worked final
find /mnt/c/Program\ Files/ -print0 2>error.txt | xargs -0 -n1 -P4 -I{} sh -c 'stat -c "%s|%n|%A|%U|%G|%y|%x" "{}" | awk -F| -v timestamp=$(date +%s%N) '\''{ gsub(/"/, "\\\\\"", $2); gsub(/"/, "\\\\\"", $6); gsub(/"/, "\\\\\"", $7); print "filesystem_stat,size="$1",permissions="$3",user="$4",group="$5" file=\"" $2 "\",modified_time=\"" $6 "\",accessed_time=\"" $7 "\" " timestamp }'\'' ' > success.txt 2>> error.txt

>>>>WORKING>>>>>>>>>>>>>>>>>>
find /mnt/c/Program\ Files/ -print0 2>error.txt | xargs -0 -n1 -P4 -I{} sh -c 'stat -c "%s:%n:%a:%U:%G:%Y:%X:%F" "{}" | awk -F: -v timestamp=$(date +%s%N) '\''{ gsub(/"/, "\\\\\"", $2); gsub(/"/, "\\\\\"", $6); gsub(/"/, "\\\\\"", $7); print "filesystem_stat,size="$1",type="$8",permissions="$3",user="$4",group="$5" file=\"" $2 "\",modified_time=\"" $6 "\",accessed_time=\"" $7 "\" " timestamp }'\'' ' > success.txt 2>> error.txt


Find cardinality
====================
grep -oP 'accessed_time=\S+' success.txt |sort |uniq|cat -n



find /mnt/c/Program\ Files/ -print0 2>error.txt | xargs -0 -n1 -P4 -I{} stat -c "%s:%n:%a:%U:%G:%Y:%X:%F" "{}"

Convert seconds from epoch to date
date -d @1687839458 +"%Y-%m-%d %H:%M:%S"

stat -c "%s %n %A %U %G %y %x" "{}" >> output.txt 2>> errors.txt:
%s: File size in bytes.
%n: File name (full file path).
%A: File permissions.
%U: User name of owner.
%G: Group name of owner.
%y: Last modified time.
%x: Last accessed time.
"{}": Placeholder for the current file.
>> output.txt: Appends the output to output.txt.
2>> errors.txt: Appends any errors (like permission denied) to errors.txt.


Tag Value: Should not contain commas, spaces, or equal signs. If the tag value contains special characters (like commas or spaces), it must be enclosed in double quotes and those special characters should be properly escaped.
Field Value: Can be a string, integer, float, or boolean.
String Values: Must be enclosed in double quotes. If the string contains double quotes, they need to be escaped with a backslash (\").
Other Values: (integer, float, boolean) do not need escaping.
Special Characters and Escaping Rules
Commas (,): Used as a delimiter between tags and fields. Should be avoided in tag keys and values without escaping.
Spaces: Should be avoided in tag keys and values without escaping.
Equal Signs (=): Used to separate tag keys from values. Should be avoided in tag keys and values without escaping.
Double Quotes ("): Used to enclose string field values. They should be escaped if they appear within the string field values.
Backslashes (): Should be escaped as double backslashes (\\) if they appear within string field values.


find /mnt/c/Program\ Files/ -print0 2>error.txt | xargs -0 -n1 -P4 -I{} sh -c '
  stat -c "%s:%n:%a:%U:%G:%Y:%X:%F" "{}" | awk -F: -v timestamp=$(date +%s%N) '\''{
    # Escape special characters for tags
	#gsub(/"/, "\\\\\"", $8); # File type
    #gsub(/"/, "\\\\\"", $2); # File path
	gsub(/,/, "\\,", $2);
	gsub(/=/, "\\=", $2);
	gsub(/ /, "\\ ", $2);
    #gsub(/"/, "\\\\\"", $3); # Permissions
    #gsub(/"/, "\\\\\"", $4); # User
    #gsub(/"/, "\\\\\"", $5); # Group

    # Output in InfluxDB Line Protocol format
    print "filesystem_stat,type=\"" $8 "\",permissions=\"" $3 "\",user=\"" $4 "\",group=\"" $5 "\",file=\"" $2 "\",modified_time=\"" $6 "\",accessed_time=\"" $7 "\" size=" $1 " " timestamp
  }'\'' ' > success.txt 2>> error.txt


>>>>WIP
find /mnt/c/Program\ Files/ -print0 2>error.txt | xargs -0 -n1 -P4 -I{} sh -c '
  file="$1";
  writable=$(test -w "$file" && echo "1" || echo "0");
  stat -c "%s:%n:%a:%U:%G:%Y:%X:%F" "{}" | awk -F: -v writable="$writable" -v timestamp=$(date +%s%N) '\''{       
    filesplit = split($2, filetype, ".");
	split($3, perm, "");
	gsub(/"/, "\\\"", $2);
	gsub(/ /, "\\ ", $2);
	gsub(/,/, "\\,", $2);
	gsub(/=/, "\\=", $2);
	gsub(/ /, "\\ ", $8);
	depth = gsub(/\//, "/", $2);	
    # Output in InfluxDB Line Protocol format
    print "filesystem_stat,type=\"" $8 "\",permissions=\"" $3 "\",userpermission=\"" perm[1] "\",grouppermission=\"" perm[2] "\",otherpermission=\"" perm[3] "\",user=\"" $4 "\",group=\"" $5 "\",file=\"" $2 "\",writable=\"" writable "\",depth=\"" depth "\",filetype=\"" filetype[filesplit] "\",modified_time=\"" $6 "\",accessed_time=\"" $7 "\" size=" $1 " " timestamp
  }'\'' ' > success.txt 2>> error.txt

>>>Match directory till double quote
grep -i directory success.txt | grep -oP 'file=".*?[^\\]"'|sed 's/\"$//'
	file="/mnt/c/Program\ Files/WSL/tr-TR
	file="/mnt/c/Program\ Files/WSL/zh-CN
	file="/mnt/c/Program\ Files/WSL/zh-TW
grep -i directory success.txt | grep -oP 'file=".*?[^\\]"'|sed 's/\"$//' | xargs -0 -n1 -P4 -I{} sh -c '
'


>>>Directory aggregation
find /path/to/directory -type f -exec stat -c "%s" {} + | awk '{s+=$1} END {print s}'
find /mnt/c/Program\ Files/ -type f -exec stat -c "%s" {} + | awk '{s+=$1} END {print s}'
>21332413370
>3min51sec
du -sh /path/to/directory
du -sh /mnt/c/Program\ Files/
>20GB
>1min4sec




#!/bin/bash

TARGET_DIR="/mnt/c/Program Files"
OUTPUT_FILE="influxdb_output.txt"
ERROR_FILE="influxdb_errors.txt"

# Ensure output files are empty
: > $OUTPUT_FILE
: > $ERROR_FILE

# Run the command
find "$TARGET_DIR" -print0 | xargs -0 -n1 -P4 -I{} sh -c '
    stat -c "%s %n %A %U %G %y %x" "{}" | awk -v timestamp=$(date +%s%N) \'{ 
        gsub(/"/, "\\\\\"", $2);
        gsub(/"/, "\\\\\"", $6);
        gsub(/"/, "\\\\\"", $7);
        print "filesystem_stat,size="$1",permissions="$3",user="$4",group="$5" file=\"" $2 "\",modified_time=\"" $6 "\",accessed_time=\"" $7 "\" " timestamp 
    }\' >> '"$OUTPUT_FILE"' 2>> '"$ERROR_FILE"'






import subprocess
import time
import datetime

def get_file_info(filepath):
    try:
        output = subprocess.check_output(["stat", "-c", "%s %n %A %U %G %y %x", filepath], text=True).strip()
        size, path, permissions, user, group, mtime, atime = output.split()
        timestamp = int(time.time() * 1000)  # Milliseconds
        return f"file_info,path={path} size={size},permissions={permissions},user={user},group={group},mtime={mtime},atime={atime} {timestamp}"
    except subprocess.CalledProcessError as e:
        print(f"Error processing file {filepath}: {e}")
        return None

def main():
    command = "find /mnt/c/Program Files/ -print0"
    output = subprocess.check_output(command, shell=True, text=True)
    file_paths = output.strip().split('\0')

    for filepath in file_paths:
        line_protocol = get_file_info(filepath)
        if line_protocol:
            print(line_protocol)

if __name__ == "__main__":
    main()



Unix script to find the directories in success.txt and for each directory found search success.txt for the regular files with filepath matching the directory path and get the sum of size of all the files and print the directory path and total size 

The success.txt contains the below data
filesystem_stat,type="directory",permissions="555",userpermission="5",grouppermission="5",otherpermission="5",user="anbu992003",group="anbu992003",file="/mnt/c/Program\ Files/7-Zip",writable="0",depth="4",filetype="/mnt/c/Program Files/7-Zip",modified_time="1625928470",accessed_time="1721562710" size=512 1721562710936900431
filesystem_stat,type="directory",permissions="555",userpermission="5",grouppermission="5",otherpermission="5",user="anbu992003",group="anbu992003",file="/mnt/c/Program\ Files/",writable="0",depth="4",filetype="/mnt/c/Program Files/",modified_time="1718344160",accessed_time="1721562710" size=512 1721562710936535426


#!/bin/bash

# Extract directories from success.txt
directories=$(grep 'type="directory"' success.txt | awk -F'file=' '{print $2}' | tr -d '"' | tr -d ',')

# Iterate through each directory
for dir in $directories; do
    # Calculate the sum of sizes of regular files within the directory
    total_size=$(grep "file=\"$dir" success.txt | grep 'type="regular file"' | awk -F'size=' '{sum += $2} END {print sum}')
    
    # Print the directory path and total size
    echo "Directory: $dir, Total Size: $total_size"
done





#!/bin/bash

# Function to extract directory path from a line
extract_dir() {
  echo "$1" | cut -d',' -f9 | cut -d'"' -f2
}

# Function to calculate total size for a directory
calculate_total_size() {
  dir_path="$1"
  total_size=0
  while IFS=',' read -r line; do
    file_path=$(extract_dir "$line")
    if [[ "$file_path" == "$dir_path"* ]]; then
      file_size=$(echo "$line" | cut -d' ' -f14)
      total_size=$(($total_size + $file_size))
    fi
  done < success.txt
  echo "$dir_path $total_size"
}

# Find unique directories
unique_dirs=$(awk -F',' '{print $9}' success.txt | cut -d'"' -f2 | sort -u)

# Calculate total size for each directory
for dir in $unique_dirs; do
  calculate_total_size "$dir"
done



