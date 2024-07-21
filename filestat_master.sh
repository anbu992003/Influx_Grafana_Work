#!/bin/bash


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
  }'\'' ' > filestat_log.txt 2>> filestat_err.txt
  
  
# Extract directories from filestat_log.txt
#directories=$(grep 'type="directory"' filestat_log.txt | awk -F'file=' '{print $2}' | tr -d '"' | tr -d ',')
#directories=$(grep 'type="directory"' filestat_log.txt | head -1 | grep -oP 'file=".*?[^\\]"'|sed 's/\"$//' | awk -F'file=\"' '{    print $2}' )
directories=$(grep 'type="directory"' filestat_log.txt | grep -oP 'file=".*?[^\\]"'|sed 's/\"$//'|sed 's/\\/\\\\\\/g' | awk -F'file=\"' '{    print $2}')
#dir=$(grep 'type="directory"' filestat_log.txt | head -1 | grep -oP 'file=".*?[^\\]"'|sed 's/\"$//'|sed 's/\\/\\\\\\/' | awk -F'file=\"' '{    print $2}')
#gsub(/[[:punct:]]/, "\\\\&", $2);
#gsub(/\\\\/, "\\", $2);
# Convert the directories variable into an array
IFS=$'\n' read -r -d '' -a directories_array <<< "$directories"

# Print the entire array (for verification)
echo "Directories array:"
for dir in "${directories_array[@]}"; do
    echo "$dir"
	# Calculate the sum of sizes of regular files within the directory
	echo "Searcing files for- $dir"
	#grep "file=\"$dir" filestat_log.txt|grep -i 'type="regular\\ file"'| grep -oP 'size=[0-9]+'| awk -F'=' '{sum += $2} END {print sum}'
    total_size=$(grep "file=\"$dir" filestat_log.txt|grep -i 'type="regular\\ file"'| grep -oP 'size=[0-9]+'| awk -F'=' '{sum += $2} END {print sum}')
    
    # Print the directory path and total size
    echo "Directory: $dir, Total Size: $total_size"
done

'''
# Iterate through each directory
for dir in $directories; do
    # Calculate the sum of sizes of regular files within the directory
    total_size=$(grep "file=\"$dir" filestat_log.txt | grep 'type="regular file"' | awk -F'size=' '{sum += $2} END {print sum}')
    
    # Print the directory path and total size
    echo "Directory: $dir, Total Size: $total_size"
done

'''



#real    3m10.272s
#user    7m51.601s
#sys     1m42.969s