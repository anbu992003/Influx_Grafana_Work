os.stat and os.walk
=========================

>>>>>>>>>>>>>>>>>>>>> File stat
import os

path = '/path/to/file_or_directory'
stat_result = os.stat(path)

print(f"Mode: {stat_result.st_mode}")
print(f"Inode: {stat_result.st_ino}")
print(f"Device: {stat_result.st_dev}")
print(f"Number of hard links: {stat_result.st_nlink}")
print(f"User ID of owner: {stat_result.st_uid}")
print(f"Group ID of owner: {stat_result.st_gid}")
print(f"Size: {stat_result.st_size} bytes")
print(f"Last accessed: {stat_result.st_atime}")
print(f"Last modified: {stat_result.st_mtime}")
print(f"Last status change: {stat_result.st_ctime}")


path
user
group
is_dir
permission >user, group, others
last_Access >sec to days
last_modified >sec to days

depth i
filecount i
dircount i
size i 

>>>>>>>>>>>>>>>>>>>> Error handling folder walk

import os

def safe_os_walk(directory_path):
    for dirpath, dirnames, filenames in os.walk(directory_path):
        try:
            # Try accessing the directory contents
            for dirname in dirnames:
                subdir_path = os.path.join(dirpath, dirname)
                try:
                    os.listdir(subdir_path)
                except PermissionError:
                    print(f"Permission denied: {subdir_path}")
                    dirnames.remove(dirname)  # Remove this directory from the list to prevent os.walk from recursing into it

            # Process files
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                try:
                    with open(file_path, 'r'):
                        pass  # Just attempt to open the file to check permissions
                except PermissionError:
                    print(f"Permission denied: {file_path}")

        except PermissionError:
            print(f"Permission denied: {dirpath}")

# Specify the directory to walk
directory_path = "/path/to/directory"

# Run the function
safe_os_walk(directory_path)

>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>User write access

import os
import pwd

def user_has_write_access(user_id, path):
    try:
        # Get the user name from user ID
        user_name = pwd.getpwuid(user_id).pw_name
    except KeyError:
        return f"User ID {user_id} not found."

    # Check if the path exists
    if not os.path.exists(path):
        return f"Path {path} does not exist."

    # Get file permissions
    file_stat = os.stat(path)
    file_mode = file_stat.st_mode

    # Get the user's UID and GID
    user_info = pwd.getpwnam(user_name)
    user_uid = user_info.pw_uid
    user_gid = user_info.pw_gid

    # Check if the user has write permission
    if (
        (file_stat.st_uid == user_uid and (file_mode & 0o200)) or  # Owner write permission
        (file_stat.st_gid == user_gid and (file_mode & 0o020)) or  # Group write permission
        (file_mode & 0o002)  # Others write permission
    ):
        return f"User {user_name} has write access to {path}."
    else:
        return f"User {user_name} does not have write access to {path}."

# Example usage
user_id = 1000  # Replace with the user ID you want to check
path = '/path/to/file_or_directory'  # Replace with the file or directory path
result = user_has_write_access(user_id, path)
print(result)
import os
import pwd

def user_has_write_access(user_id, path):
    try:
        # Get the user name from user ID
        user_name = pwd.getpwuid(user_id).pw_name
    except KeyError:
        return f"User ID {user_id} not found."

    # Check if the path exists
    if not os.path.exists(path):
        return f"Path {path} does not exist."

    # Get file permissions
    file_stat = os.stat(path)
    file_mode = file_stat.st_mode

    # Get the user's UID and GID
    user_info = pwd.getpwnam(user_name)
    user_uid = user_info.pw_uid
    user_gid = user_info.pw_gid

    # Check if the user has write permission
    if (
        (file_stat.st_uid == user_uid and (file_mode & 0o200)) or  # Owner write permission
        (file_stat.st_gid == user_gid and (file_mode & 0o020)) or  # Group write permission
        (file_mode & 0o002)  # Others write permission
    ):
        return f"User {user_name} has write access to {path}."
    else:
        return f"User {user_name} does not have write access to {path}."

# Example usage
user_id = 1000  # Replace with the user ID you want to check
path = '/path/to/file_or_directory'  # Replace with the file or directory path
result = user_has_write_access(user_id, path)
print(result)
