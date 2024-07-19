import os
import pwd
import grp
import time

def get_file_info(path):
    try:
        stat_info = os.stat(path)
        size = stat_info.st_size
        owner = pwd.getpwuid(stat_info.st_uid).pw_name
        group = grp.getgrgid(stat_info.st_gid).gr_name
        permissions = oct(stat_info.st_mode)[-3:]
        return size, owner, group, permissions
    except Exception as e:
        return None, None, None, None

def walk_directory(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            path = os.path.join(root, file)
            yield path

def main():
    base_directory = "/"  # Set to the root directory
    current_time = int(time.time() * 1000000000)  # Nanoseconds since epoch
    
    with open("/tmp/file_info.lp", "w") as f:
        for path in walk_directory(base_directory):
            size, owner, group, permissions = get_file_info(path)
            if size is not None:
                line = f'file_info,path="{path}",owner="{owner}",group="{group}",permissions="{permissions}" size={size}i {current_time}\n'
                f.write(line)
                current_time += 1  # Increment time for unique timestamps

if __name__ == "__main__":
    main()






[[inputs.exec]]
  commands = ["cat /tmp/file_info.lp"]
  data_format = "influx"
  interval = "1h"  # Adjust the interval as needed

[[outputs.influxdb]]
  urls = ["http://localhost:8086"]
  database = "exampleDB"
  username = "your_username"
  password = "your_password"


influx -host localhost -port 8086 -database 'exampleDB'
> SHOW MEASUREMENTS
> SELECT * FROM file_info LIMIT 10



# Parse a complete file each interval
[[inputs.file]]
  ## Files to parse each interval.  Accept standard unix glob matching rules,
  ## as well as ** to match recursive files and directories.
  files = ["/tmp/metrics.out"]
  data_format = "influx"
  
[[inputs.tail]]
  files = ["/path/to/datafile.txt"]
  from_beginning = true
  name_override = "custom_data"
  data_format = "influx"


anbu992003@LAPTOP-ODISB5A1:~$ nohup python3 fileinfo.py 2>/dev/null &
[1] 136