import os
import multiprocessing
import stat

def get_size(start_path='.'):
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(start_path):
            for file in filenames:
                fp = os.path.join(dirpath, file)
                try:
                    file_size = os.path.getsize(fp)
                    print(">>path:" + fp+ " >>filesize:" + str(file_size))
                    total_size += file_size
                except OSError as e:
                    if e.errno == errno.EACCES:
                        print(f"Permission denied for {fp}")
                    else:
                        raise
            for dirname in dirnames:
                # Handle subdirectories recursively
                sub_path = os.path.join(dirpath, dirname)
                try:
                    sub_size = get_size(sub_path)
                    total_size += sub_size
                except OSError as e:
                    if e.errno == errno.EACCES:
                        print(f"Permission denied for {sub_path}")
                    else:
                        raise
    except OSError as e:
        if e.errno == errno.EACCES:
            print(f"Permission denied for {start_path}")
        else:
            raise
    return total_size

def read_file_to_list(file_path):
    """Read a file and save each line into a list."""
    lines = []
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
    except IOError as e:
        print(f"Error: An I/O error occurred while reading {file_path}: {e}")
    return lines

def main():
    #start_path = "/mnt/c/Program Files/"  # Replace with your desired path
    folder_list = read_file_to_list('/home/anbu992003/subdirectories.txt')
    print(folder_list)
    # Parallelize the process (adjust the number of processes as needed)
    with multiprocessing.Pool(processes=8) as pool:
        #results = pool.map(get_size, [start_path])
        results = pool.map(get_size, folder_list)

    print(f"Total size: {results[0]} bytes")

if __name__ == "__main__":
    main()