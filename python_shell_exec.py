import subprocess

def execute_and_process():
    # Define the shell command
    command = "find /path/to/directory -mindepth 1 -maxdepth 1 -type d -print0 | xargs -0 -n1 -P4 du -sh"
    
    # Execute the command using subprocess
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Process the output line by line
    for line in process.stdout:
        # Split the output (e.g., "4.0K    /path/to/directory/subdir")
        size, directory = line.strip().split('\t')
        
        # Print or process the output as needed
        print(f"Directory: {directory}, Size: {size}")
    
    # Check for any errors
    stderr = process.stderr.read()
    if stderr:
        print(f"Errors:\n{stderr}")

if __name__ == "__main__":
    execute_and_process()










import subprocess

def process_directory_sizes(directory_path):
  """
  Executes the shell command and processes the output.

  Args:
    directory_path: The path to the directory to process.

  Returns:
    A list of tuples, where each tuple contains the directory size and name.
  """

  command = f"find {directory_path} -mindepth 1 -maxdepth 1 -type d -print0 | xargs -0 -n1 -P4 du -sh"
  result = subprocess.run(command, shell=True, capture_output=True, text=True)

  if result.returncode != 0:
    print(f"Error executing command: {result.stderr}")
    return []

  directory_sizes = []
  for line in result.stdout.splitlines():
    size, directory = line.split('\t')
    directory_sizes.append((size, directory))

  return directory_sizes

# Example usage:
directory_path = "/path/to/directory"
sizes = process_directory_sizes(directory_path)
for size, directory in sizes:
  print(f"{size}\t{directory}")



