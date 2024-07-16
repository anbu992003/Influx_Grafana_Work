import os
import multiprocessing
import stat
import pwd
import json

def get_file_stats(file_path):
    try:
        stats = os.stat(file_path)
        file_info = {
            'path': file_path,
            'size': stats.st_size,
            'owner': pwd.getpwuid(stats.st_uid).pw_name,
            'permissions': stat.filemode(stats.st_mode)
        }
        return file_info
    except Exception as e:
        print(f"Error getting stats for {file_path}: {e}")
        return None

def process_files(file_paths):
    results = []
    for file_path in file_paths:
        file_stats = get_file_stats(file_path)
        if file_stats:
            results.append(file_stats)
    return results

def get_all_file_paths(directory):
    file_paths = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths

def parallel_process_files(file_paths, num_workers):
    chunk_size = len(file_paths) // num_workers
    chunks = [file_paths[i:i + chunk_size] for i in range(0, len(file_paths), chunk_size)]

    with multiprocessing.Pool(num_workers) as pool:
        results = pool.map(process_files, chunks)

    # Flatten the list of results
    flat_results = [item for sublist in results for item in sublist]
    return flat_results

if __name__ == "__main__":
    directory_to_scan = "/path/to/scan"  # Change this to the directory you want to scan
    output_file = "filesystem_stats.json"
    num_workers = multiprocessing.cpu_count()

    print("Gathering file paths...")
    file_paths = get_all_file_paths(directory_to_scan)

    print("Processing files in parallel...")
    file_stats = parallel_process_files(file_paths, num_workers)

    print(f"Writing results to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(file_stats, f, indent=4)

    print("Done.")
